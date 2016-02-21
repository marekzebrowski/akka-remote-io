package pl.inti

import java.net.{InetAddress, InetSocketAddress}

import akka.OnlyCauseStackTrace
import akka.actor._
import akka.io.Inet.SO.ReuseAddress
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.pattern.ask
import akka.remote.transport.AssociationHandle.{Disassociated, HandleEventListener, InboundPayload}
import akka.remote.transport.Transport.{AssociationEventListener, InboundAssociation}
import akka.remote.transport.{AssociationHandle, Transport}
import akka.util.{Timeout, ByteString}
import scala.concurrent.duration._

import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future, Promise}

@SerialVersionUID(1L)
class RemoteIoTransportException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) with OnlyCauseStackTrace {
  def this(msg: String) = this(msg, null)
}


class IOAssociacionHandle(local: Address, remote: Address, connection: ActorRef) extends AssociationHandle {

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

  //lots of copying... - maybe
  def framed(payload: ByteString): ByteString = {
    val len = payload.length
    val frameSizeBS = ByteString((len >>> 24).toByte, (len >>> 16).toByte, (len >>> 8).toByte, len.toByte)
    frameSizeBS ++ payload
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
  var buffer: ByteString = ByteString.empty


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
      p.failure(new RemoteIoTransportException(s"AssocActor - failed to connect to ${remoteAddr}"))

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
      ah.readHandlerPromise.future.foreach(_.notify(Disassociated(AssociationHandle.Shutdown)))
      context stop self

  }

  def messageLength(): Int = {
    if (buffer.length < 4) Integer.MAX_VALUE - 4
    else {
      (buffer(0) & 0xff) << 24 |
        (buffer(1) & 0xff) << 16 |
        (buffer(2) & 0xff) << 8 |
        buffer(3) & 0xff
    }
  }

  def consumeBuffer(): Unit = {
    val len = messageLength()
    val frameLen = len + 4
    if (buffer.length >= frameLen) {
      val (consumed, remaining) = buffer.splitAt(frameLen)
      val payload = consumed.slice(4, frameLen)
      if (payload.nonEmpty) {
        ah.readHandlerPromise.future.foreach(_.notify(InboundPayload(payload)))
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

  val Port: Int = config.getInt("port")
  val UseDispatcherForIo: Option[String] = config.getString("use-dispatcher-for-io") match {
    case "" | null ⇒ None
    case dispatcher ⇒ Some(dispatcher)
  }
  val Backlog: Int = config.getInt("backlog")
}

case class Associate(remoteAddr: Address, p: Promise[AssociationHandle])
case object RemoteIOShutdown

class IORemote(system: ExtendedActorSystem, conf: Config) extends Transport {
  val settings = new IORemoteSettings(conf)
  implicit val _system = system
  // no access to RARP - we are outside akka, so allow only own dispatcher
  implicit val ec: ExecutionContext = settings.UseDispatcherForIo.map(system.dispatchers.lookup).getOrElse(system.dispatcher)

  val ioManager = IO(Tcp)

  var localAddr: Address = _

  var handler: ActorRef = ActorRef.noSender

  override def schemeIdentifier: String = "tcp"

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    val associationListenerPromise: Promise[AssociationEventListener] = Promise()
    val addressPromise = Promise[Address]
    val props = Props(classOf[BindActor], addressPromise, associationListenerPromise.future, schemeIdentifier, system.name, settings)
    val propsWithDispatcher = settings.UseDispatcherForIo match {
      case Some(dispatcherName) => props.withDispatcher(dispatcherName)
      case None => props
    }
    handler = system.actorOf(propsWithDispatcher, "remote-io-handler")
    addressPromise.future.map { af =>
      (af, associationListenerPromise)
    }
  }

  override def shutdown(): Future[Boolean] = {
    implicit val timeout = Timeout(1 minute)
    system.log.info("RemoteIO system shutting down ...")
    (handler ? RemoteIOShutdown).mapTo[Boolean]
  }

  //TODO - configurable
  override def maximumPayloadBytes: Int = Int.MaxValue

  override def isResponsibleFor(address: Address): Boolean = true

  override def associate(remoteAddr: Address): Future[AssociationHandle] = {
    val p = Promise[AssociationHandle]
    handler ! Associate(remoteAddr, p)
    p.future
  }
}

class BindActor(addressPromise: Promise[Address], assocListenerF: Future[AssociationEventListener], schemeIdentifier: String, systemName: String, settings: IORemoteSettings) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher
  var address: Address = null
  val bindLocalAddress = new InetSocketAddress(settings.Hostname, settings.Port)
  val options = List(ReuseAddress(true))
  IO(Tcp)(context.system) ! Bind(self, bindLocalAddress, settings.Backlog, options, false)
  var tcpActor = ActorRef.noSender
  def receive = {
    case c@Connected(remote, local) =>
      val connection = sender()
      val remoteAddr = Address(schemeIdentifier, systemName, remote.getHostString, remote.getPort)
      val ah = new IOAssociacionHandle(address, remoteAddr, connection)
      val handler = context.actorOf(AssocActor.props(address, remoteAddr, ah), s"conn-${remote.getHostName}-${remote.getPort}")
      connection ! Register(handler)
      assocListenerF.foreach(_.notify(InboundAssociation(ah)))

    case Associate(remoteAddr, p) =>
      context.actorOf(AssocActor.props(address, remoteAddr, p), s"conn-${remoteAddr.host.getOrElse("")}-${remoteAddr.port.getOrElse(0)}")

    case Bound(localAddress) =>
      tcpActor = sender()
      address = Address(schemeIdentifier, systemName, localAddress.getHostString, localAddress.getPort)
      addressPromise.success(address)

    case CommandFailed(b: Bind) =>
      log.warning("Bind Actor CommandFailed -don't know what to do")
      addressPromise.failure(new RemoteIoTransportException(s"cannot bind IO to ${b.localAddress}"))
      context stop self

    case RemoteIOShutdown =>
      //context.children foreach (child => child ! Disconnect - somehow)
      tcpActor ! Unbind
      context setReceiveTimeout (1 minute)
      context become unbinding(sender())
    }

  def unbinding(replyTo:ActorRef): Receive = {
    case Tcp.Unbound =>
      log.info("Successful unbind")
      replyTo ! true
      context stop self

    case ReceiveTimeout =>
      log.info("Cannot unbind in time")
      replyTo ! false
      context stop self
  }

}