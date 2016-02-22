//source https://gist.github.com/ibalashov/381f323ca976c3364c84
package akkatest

import java.util.logging.Logger

import akka.actor._
import akka.routing.RoundRobinPool
import com.typesafe.config.{Config, ConfigFactory}
import akka.serialization._
import scala.util.Try

class TestSerializer extends Serializer {
  def identifier = 200
  def includeManifest: Boolean = false
  def toBinary(obj: AnyRef): Array[Byte] = {
    obj match {
      case PingObj => {
        val a = new Array[Byte](1)
        a(0)=1
        a
      }
      case PongObj => {
        val a = new Array[Byte](1)
        a(0)=2
        a
      }
      case _ => throw new RuntimeException("not serializable")
    }
  }
  def fromBinary(bytes: Array[Byte],
                 clazz: Option[Class[_]]): AnyRef = {
      bytes(0) match {
        case 1 => PingObj
        case 2 => PongObj
        case _ => throw new RuntimeException("Not unserializable")
      }
  }
}

sealed trait PP extends Serializable
case object PingObj extends PP
case object PongObj extends PP

object TestSystem  {


  class ActorA extends Actor with MeasureInBatches {
    override def receive: Receive = {
      case PingObj =>
        sender() ! PongObj
        measure()
    }
  }

  class ActorB extends Actor with MeasureInBatches {
    override def receive: Receive = {
      case _ => measure()
    }
  }

  val logger = Logger.getLogger("akkatest")

  def main(args: Array[String]): Unit = {
    val port: Int = args(0).toInt
    val localIpAddress: String = "127.0.0.1"

    val nettyConf: String =
      s"""
        |akka.remote.netty.tcp.port = $port
        |akka.remote.netty.tcp.hostname = $localIpAddress
      """.stripMargin

    val remoteIOConf: String =
      s"""
         |akka.remote {
         |    enabled-transports = ["akka.remote.ioremote.tcp"]
         |    ioremote.tcp.port = $port
         |    ioremote.tcp.hostname = $localIpAddress
         |}
       """.stripMargin

    val transportConf = remoteIOConf // nettyConf|remoteIOConf
    val commonRemotingConfig: Config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.actor.serializers.myown = "akkatest.TestSerializer"
         |akka.actor.serialization-bindings {
         |    "akkatest.PP" = myown
         |}
         |network-dispatcher {
         |      type = Dispatcher
         |      executor = "fork-join-executor"
         |      fork-join-executor {
         |        parallelism-min = 1
         |        parallelism-max = 1
         |      }
         |      throughput = 1000
         |}
         |akka.remote.use-dispatcher = "network-dispatcher"
         |akka.remote.use-dispatcher-for-io = "network-dispatcher"
         |${transportConf}
      """.stripMargin)

    if (args.length == 1) {
      val config: Config = ConfigFactory.parseString(
        s"""|
            |server-dispatcher {
            |     type = Dispatcher
            |     executor = "fork-join-executor"
            |     fork-join-executor {
            |       parallelism-min = 1
            |       parallelism-max = 1
            |     }
            |     throughput = 1000
            |}
      """.stripMargin)
      ActorSystem("systemA", config.withFallback(commonRemotingConfig))
        .actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors()) // 4 cores
          .props(Props(classOf[ActorA])
          .withDispatcher("server-dispatcher")),
          "actorA")


    } else if (args.length >= 2) {
      val config: Config = ConfigFactory.parseString(
        s"""
           |client-dispatcher {
           |     type = Dispatcher
           |     executor = "fork-join-executor"
           |     fork-join-executor {
           |       parallelism-min = 1
           |       parallelism-max = 1
           |     }
           |     throughput = 1000
           |}
      """.stripMargin)

      val remoteActorPath: String = args(1)
      val actorSystem: ActorSystem = ActorSystem("systemB", config.withFallback(commonRemotingConfig))
      val actorAsel: ActorSelection = actorSystem.actorSelection(remoteActorPath)

      def sendFromActor(actor: ActorRef, messages: Int): Unit = {
          (1 to messages) foreach { x =>
            actorAsel.tell(PingObj, actor)
          }
          logger.info(s"Done sending: $messages messages from: $actor to: $actorAsel")
      }
      val messages: Int = Try {
        args(2).toInt
      }.getOrElse(1000000)

      sendFromActor(actorSystem.actorOf(RoundRobinPool(Runtime.getRuntime.availableProcessors()).props(
        Props(classOf[ActorB]).withDispatcher("client-dispatcher"))), messages)
    }
  }
}

trait MeasureInBatches {
  val logger = Logger.getLogger(this.getClass.getSimpleName)
  var batchSz: Long = 10000
  var cursor: Long = 0
  var lastTs: Option[Long] = None

  def measure() = {
    if (cursor % batchSz == 0) {
      if (lastTs.isDefined) {
        val ms: Some[Long] = getCurrentTimeMs
        val tsDiff: Long = ms.get - lastTs.get
        val throughput: Double = batchSz.toDouble / tsDiff * 1000
        logger.info(s"$this $cursor: $cursor, last batch throughput: $throughput")
      }
      lastTs = getCurrentTimeMs
    }
    cursor += 1
  }

  def getCurrentTimeMs: Some[Long] = {
    Some(System.nanoTime() / 1000000)
  }
}
