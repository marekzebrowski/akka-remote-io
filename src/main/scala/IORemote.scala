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
  import scala.concurrent.ExecutionContext.Implicits.global
  val rhp = Promise[HandleEventListener]()
  rhp.future.onComplete{
    case x => println(s"rhp completed with ${x}")
  }
  override def localAddress: Address = local
  override def remoteAddress: Address = remote
  override def disassociate(): Unit = {
    println("disassociate")
    connection ! Close
  }
  override def write(payload: ByteString): Boolean = {
    println("write payload")
    connection ! Write(payload)
    true
  }
  override def readHandlerPromise: Promise[HandleEventListener] = {
    println("giving rhp back")
    rhp
  }
}


class AssocActor(localAddr: Address, remoteAddr: Address, p: Promise[AssociationHandle]) extends Actor with ActorLogging {

  import context.system
  implicit val ec = context.dispatcher

  val socketAddress = new InetSocketAddress(remoteAddr.host.get, remoteAddr.port.get)
  IO(Tcp) ! Connect(socketAddress)

  def receive = {
    case CommandFailed(_: Connect) =>
      context stop self
      p.failure(new RuntimeException("AssocActor - failed to connect"))

    case c@Connected(remote, local) =>
      val connection = sender()
      log.info("AssocActor - connected")
      val ah = new IOAssociacionHandle(localAddr, remoteAddr, connection)
      p.success(ah)
      connection ! Register(self)
      context become {
        case CommandFailed(w: Write) =>
          // O/S buffer was full
          log.warning("AssocActor = write buffer full {}", w)
        case Received(data) =>
          log.info("AssocActor - received")
          ah.readHandlerPromise.future.map{f =>
            log.info("AssocActor - Inbound payload  ")
            f.notify(InboundPayload(data))}

        case _: ConnectionClosed =>
          log.info("AssocActor - conn closed")
          ah.readHandlerPromise.future.map(f => f.notify(Disassociated(AssociationHandle.Shutdown)))
          context stop self

        case Write(x, y) =>
          log.warning("Assoc actor received Write")
      }
  }
}

class IncomingAssocActor extends Actor with ActorLogging {
  implicit val ec = context.dispatcher

  var ah: IOAssociacionHandle = null
  def receive = {
    case CommandFailed(w: Write) =>
      // O/S buffer was full
      log.warning("AssocActor = write buffer full {}", w)
    case Received(data) =>
      log.info("IncomingAssocActor - Received")
      ah.readHandlerPromise.future.map { f =>
        log.info("IncomingAssocActor - Inbound payload  ")
        f.notify(InboundPayload(data))
      }

    case _: ConnectionClosed =>
      log.info("IncomingAssocActor - conn closed")
      ah.readHandlerPromise.future.map { f =>
        f.notify(Disassociated(AssociationHandle.Shutdown))
      }
      context stop self

    case h : IOAssociacionHandle =>
      log.info("IncomingAssocActor - install new assocHanlde")
      ah = h

    case Write(x, y) =>
      log.warning("IncomingAssocActor actor received Write")


  }
}


class IORemoteSettings(config: Config) {
  val Hostname: String = config.getString("hostname") match {
    case ""    ⇒ InetAddress.getLocalHost.getHostAddress
    case value ⇒ value
  }

  val port: Int = config.getInt("port")
}


class IORemote(system: ExtendedActorSystem, conf: Config) extends Transport {
  system.log.info("IO Remote starts ...")
  val settings = new IORemoteSettings(conf)
  implicit val _system = system
  implicit val ec = system.dispatcher

  val ioManager = IO(Tcp)

  var localAddr: Address = _

  override def schemeIdentifier: String = "tcp"

  override def listen: Future[(Address, Promise[AssociationEventListener])] = {
    system.log.info("Listen called")
    val associationListenerPromise: Promise[AssociationEventListener] = Promise()
    val addressPromise = Promise[Address]
    val handler = system.actorOf(Props(classOf[BindActor], addressPromise, associationListenerPromise.future, schemeIdentifier, system.name, settings))
    addressPromise.future.map{af =>
      println("addressPromise fulfilled")
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

  override def associate(remoteAddress: Address): Future[AssociationHandle] = {
    val p = Promise[AssociationHandle]
    system.actorOf(Props(classOf[AssocActor], localAddr, remoteAddress, p))
    p.future
  }
}

class BindActor(addressPromise: Promise[Address], assocListenerF: Future[AssociationEventListener], schemeIdentifier: String, systemName: String, settings: IORemoteSettings) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher
  var address: Address = null
  log.info("Bind actor started")
  log.info("listen handler created")
  val localAddress = new InetSocketAddress(settings.Hostname, settings.port)
  log.info("local addr {}:{}",localAddress.getHostName, localAddress.getPort)
  val options = List(ReuseAddress(true))
  val backlog = 100
  val pullMode = false
  log.info(s"Binding to {}", localAddress)
  IO(Tcp)(context.system) ! Bind(self, localAddress, backlog, options, pullMode)
  def receive = {
    case Bound(localAddress) =>
      println("BOUND")
      log.warning("Bind Actor bound {}", localAddress)
      address = Address(schemeIdentifier, systemName, localAddress.getHostString, localAddress.getPort)
      addressPromise.success(address)

    case CommandFailed(_: Bind) => context stop self
      log.warning("Bind Actor CommandFailed -don't know what to do")
      addressPromise.failure(new IllegalArgumentException("cannot bind IO"))

    case c@Connected(remote, local) =>
      val connection = sender()
      log.warning("Bind Actor connected remote {} local {}", remote, local)
      val handler = context.actorOf(Props[IncomingAssocActor])
      connection ! Register(handler)
      val remoteAddr = Address(schemeIdentifier, systemName, remote.getHostString, remote.getPort)
      if(!assocListenerF.isCompleted) {
        log.warning("ALF not complete - can't handle connections")
      }
      assocListenerF.map { f =>
        log.info("ALF - new IOAssocHandle")
        val ah = new IOAssociacionHandle(address, remoteAddr, connection)
        handler ! ah
        f.notify(InboundAssociation(ah))
      }
    case x =>
      log.warning("Binder - unknown message {}",x)
  }
}