package pl.inti

import java.net.{InetAddress, InetSocketAddress}

import akka.actor._
import akka.io.Inet.SO.ReuseAddress
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.ByteString
import com.typesafe.config.Config

import scala.concurrent.{Future, Promise}


class IOAssociacionHandle(local: Address, remote: Address, connection: ActorRef) extends AssociationHandle {

  // you are creating lots of objects anyway, so buffering this array is not worth it. I would just allocate it in framed()
  val lenBuffer = new Array[Byte](4)

  val rhp = Promise[HandleEventListener]()

  override def localAddress: Address = local

  override def remoteAddress: Address = remote

  override def disassociate(): Unit = {
    connection ! Close
  }

  override def write(payload: ByteString): Boolean = {
    connection ! Write(framed(payload))
    true
  }

  override def readHandlerPromise: Promise[HandleEventListener] = rhp


  // don't worry too much about the copying. ByteString is a rope-like data structure, so the payload is not actually copied
  //lots of copying...
  def framed(payload: ByteString): ByteString = {
    val len = payload.length
    lenBuffer(0) = (len >>> 24).toByte
    lenBuffer(1) = (len >>> 16).toByte
    lenBuffer(2) = (len >>> 8).toByte
    lenBuffer(3) = len.toByte
    ByteString(lenBuffer) ++ payload
  }
}


object AssocActor {

  sealed trait AssocStart

  case class StartConnected(ah: IOAssociacionHandle) extends AssocStart

  case object StartConnecting extends AssocStart

  def props(localAddr: Address, remoteAddr: Address, p: Promise[AssociationHandle]) =
    Props(classOf[AssocActor], localAddr, remoteAddr, StartConnecting, p)


  def props(localAddr: Address, remoteAddr: Address, ah: IOAssociacionHandle) =
    Props(classOf[AssocActor], localAddr, remoteAddr, StartConnected(ah), Promise.successful(ah))

}

class AssocActor(localAddr: Address, remoteAddr: Address, startMessage: AssocActor.AssocStart, p: Promise[AssociationHandle]) extends Actor with ActorLogging {

  import AssocActor._
  import context.system

  implicit val ec = context.dispatcher
  var ah: IOAssociacionHandle = _
  // it seems to me that this is only being accessed from within the actor. So why @volatile?
  @volatile var buffer: ByteString = ByteString.empty
  // this saves one allocation. But with all the copying that is going on all over the place, I would guess that it is
  // not worth it.
  val lenBytes = new Array[Byte](4)


  val socketAddress = new InetSocketAddress(remoteAddr.host.get, remoteAddr.port.get)
  IO(Tcp) ! Connect(socketAddress)

  self ! startMessage

  //default - can start connected or start connecting
  def receive = {
    case StartConnecting =>
      context become connecting
    case StartConnected(handle) =>
      ah = handle
      context become connected
  }

  def connecting: Receive = {
    case CommandFailed(_: Connect) =>
      context stop self
      p.failure(new RuntimeException("AssocActor - failed to connect"))

    case c@Connected(remote, local) =>
      val connection = sender()
      ah = new IOAssociacionHandle(localAddr, remoteAddr, connection)
      p.success(ah)
      connection ! Register(self)
      context become connected
  }


  def connected: Receive = {
    case CommandFailed(w: Write) =>
      // O/S buffer was full
      log.warning("AssocActor = write buffer full {}", w)
    case Received(data) =>
      buffer = buffer ++ data
      consumeBuffer()

    case _: ConnectionClosed =>
      // mapping a future without storing the result. 1. use foreach? 2. what does it do?
      ah.readHandlerPromise.future.map(f => f.notify(Disassociated(AssociationHandle.Shutdown)))
      context stop self
  }

  def messageLength(): Int = {
    if (buffer.length < 4) Integer.MAX_VALUE - 4
    else {
      buffer.copyToArray(lenBytes, 0, 4)
      (lenBytes(0) & 0xff) << 24 |
        (lenBytes(1) & 0xff) << 16 |
        (lenBytes(2) & 0xff) << 8 |
        lenBytes(3) & 0xff
    }
  }

  def consumeBuffer(): Unit = {
    val len = messageLength()
    val frameLen = len + 4
    if (buffer.length >= frameLen) {
      val (consumed, remaining) = buffer.splitAt(frameLen)
      val payload = consumed.slice(4, frameLen)
      // again, mapping without storing the result. Foreach?
      ah.readHandlerPromise.future.map { f =>
        if(payload.nonEmpty) f.notify(InboundPayload(payload))
      }
      buffer = remaining
      if (buffer.nonEmpty) consumeBuffer()
    }
  }
}


class IORemoteSettings(config: Config) {
  val Hostname: String = config.getString("hostname") match {
    case "" ⇒ InetAddress.getLocalHost.getHostAddress
    case value ⇒ value
  }

  val port: Int = config.getInt("port")
}


class IORemote(system: ExtendedActorSystem, conf: Config) extends Transport {
  val settings = new IORemoteSettings(conf)
  implicit val _system = system
  implicit val ec = system.dispatcher

  val ioManager = IO(Tcp)

  var localAddr: Address = _

  override def schemeIdentifier: String = "tcp"

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    val associationListenerPromise: Promise[AssociationEventListener] = Promise()
    val addressPromise = Promise[Address]
    val handler = system.actorOf(Props(classOf[BindActor], addressPromise, associationListenerPromise.future, schemeIdentifier, system.name, settings))
    addressPromise.future.map { af =>
      (af, associationListenerPromise)
    }
  }

  override def shutdown(): Future[Boolean] = {
    system.log.info("Shut down everything ...")
    Future.successful(true)
  }

  //TODO - configurable
  override def maximumPayloadBytes: Int = Int.MaxValue

  override def isResponsibleFor(address: Address): Boolean = true

  override def associate(remoteAddr: Address): Future[AssociationHandle] = {
    val p = Promise[AssociationHandle]
    system.actorOf(AssocActor.props(localAddr, remoteAddr, p))
    p.future
  }
}

class BindActor(addressPromise: Promise[Address], assocListenerF: Future[AssociationEventListener], schemeIdentifier: String, systemName: String, settings: IORemoteSettings) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher
  var address: Address = null
  val localAddress = new InetSocketAddress(settings.Hostname, settings.port)
  val options = List(ReuseAddress(true))
  val backlog = 100
  val pullMode = false
  IO(Tcp)(context.system) ! Bind(self, localAddress, backlog, options, pullMode)

  def receive = {
    case Bound(localAddress) =>
      address = Address(schemeIdentifier, systemName, localAddress.getHostString, localAddress.getPort)
      addressPromise.success(address)

    case CommandFailed(_: Bind) => context stop self
      log.warning("Bind Actor CommandFailed -don't know what to do")
      addressPromise.failure(new IllegalArgumentException("cannot bind IO"))

    case c@Connected(remote, local) =>
      val connection = sender()
      val remoteAddr = Address(schemeIdentifier, systemName, remote.getHostString, remote.getPort)
      val ah = new IOAssociacionHandle(address, remoteAddr, connection)
      val handler = context.actorOf(AssocActor.props(address, remoteAddr, ah))
      connection ! Register(handler)
      assocListenerF.map { f =>
        f.notify(InboundAssociation(ah))
      }
  }
}
