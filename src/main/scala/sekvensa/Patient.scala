package sekvensa

import akka.actor._
import com.codemettle.reactivemq._
import com.codemettle.reactivemq.ReActiveMQMessages._
import com.codemettle.reactivemq.model._
import com.typesafe.config.ConfigFactory
import elastic.Searcher
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import com.github.nscala_time.time.Imports._

import scala.util.parsing.json.JSON

/**
  * Created by elin on 2016-02-12.
  */
class Patient extends Actor {
  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all // for json serialization

  // reading from config file
  val config = ConfigFactory.load()
  val address = config.getString("sp.activemq.address")
  val user = config.getString("sp.activemq.user")
  val pass = config.getString("sp.activemq.pass")
  val readFrom = config.getString("sp.simpleservice.readFromTopic")
  val writeTo = config.getString("sp.simpleservice.writeToTopic")
  
  val searcher = new Searcher

  //The state
  var theBus: Option[ActorRef] = None
  //var currentState: List[sekvensa.service.ElvisPatient] = List()

  def receive = {
    case "connect" => {
      ReActiveMQExtension(context.system).manager ! GetAuthenticatedConnection(s"nio://$address:61616", user, pass)
    }
    case ConnectionEstablished(request, c) => {
      println("connected:" + request)
      c ! ConsumeFromTopic(readFrom)
      println("cons:" + c)
      theBus = Some(c)
      println("bus:" + theBus)
    }
    case ConnectionFailed(request, reason) => {
      println("failed:" + reason)
    }
    case mess @ AMQMessage(body, prop, headers) => {
      searcher.postEventToElastic(body.toString)
      println(body)
    }
  }
}

object Patient {
  def props = Props[Patient]
}
