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

  // The state
  var theBus: Option[ActorRef] = None
  var currentState: List[ElvisPatient] = List()


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
    case mess@AMQMessage(body, prop, headers) => {
    }
  }
}

object Patient {
  def props = Props[Patient]
}
