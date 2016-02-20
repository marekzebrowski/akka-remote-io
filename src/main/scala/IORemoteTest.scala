package pl.inti

import akka.actor._
import com.typesafe.config.ConfigFactory

import scala.concurrent.Promise
import scala.io.StdIn
import scala.concurrent.ExecutionContext.Implicits.global
class EchoActor extends Actor {
  var cnt = 0
  def receive = {
    case x: String =>
      val s= sender()
      println(s"Replying with ${x} ${cnt}")
      cnt = cnt + 1
      s ! x
  }
}



case class SendN(n: Int)

class SenderActor(as: ActorSelection, p: Promise[Unit]) extends Actor with ActorLogging {
  var rcvCounter = 0
  var expCounter = 0
  def receive = {
    case SendN(cnt) =>
      expCounter = cnt
      for(i <- 1 to cnt) {
        println(s"Sending msg ${i} ")
        as ! s"message ${i}"
      }
      println(s"exp coutner set to ${expCounter}")
    case x:String =>
      val s= sender()
      rcvCounter = rcvCounter + 1
      if(rcvCounter >= expCounter) p.success(())
      log.info("got string from {}, {}, counter is {}",s, x, rcvCounter)


  }
}

object IORemoteTest {
  def main(args: Array[String]):Unit = {
    println(s"args ${args.length} ${args(0)}")
    if(args.length >= 1 &&  args(0) == "server") {
      println("Start server")
        val system = ActorSystem("iooi", ConfigFactory.parseString(
          """
            |akka {
            |  actor {
            |    provider = "akka.remote.RemoteActorRefProvider"
            |  }
            |  remote {
            |    enabled-transports = ["akka.remote.ioremote.tcp"]
            |    ioremote.tcp.hostname = "127.0.0.1"
            |    }
            |  }
          """.stripMargin))
        system.actorOf(Props[EchoActor], "echo")
        //wait for key
        println("Initialized, waiting for messages")
        StdIn.readLine()
        system.terminate()
      } else {
      println("Start client")
        val system = ActorSystem("iooi", ConfigFactory.parseString(
          """
            |akka {
            |  actor {
            |    provider = "akka.remote.RemoteActorRefProvider"
            |  }
            |  remote {
            |    enabled-transports = ["akka.remote.ioremote.tcp"]
            |    ioremote.tcp.port = 2553
            |    ioremote.tcp.hostname = "127.0.0.1"
            |    }
            |  }
          """.stripMargin))
        val p = Promise[Unit]()
        val as = system.actorSelection("akka.tcp://iooi@127.0.0.1:2552/user/echo")
        val client = system.actorOf(Props(classOf[SenderActor], as, p))
        client ! SendN(10)
        p.future.map{ _ =>
          println("Comms complete")
        }
        StdIn.readLine()
        system.terminate()
     }
  }
}
