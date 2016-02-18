package elastic

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import wabisabi._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.parsing.json.JSON
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import scala.concurrent.ExecutionContext.Implicits.global

class Searcher {
  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all // for json serialization

  val config = ConfigFactory.load()
  val ip = config.getString("elastic.ip")
  val port = config.getString("elastic.port")

  val client = new Client(s"http://$ip:$port")
  println("Searcher instance connecting to " + ip + ":" + port )


  def postJsonToElastic(message: String) {
    println("\nSearcher received a message!")
    println("MESSAGE: "+ message)
    val messageMap = jsonToMap(message)
    val isa = jsonExtractor(messageMap, List("isa"))
    println("isa: " + isa )
    isa match {
      // these first two should do the same thing i think
      case "newLoad" => {
        postEntirePatientToElastic(messageMap)
      }
      case "new" => {
        postEntirePatientToElastic(messageMap)
      }
      case "diff" => {
        diffPatient(messageMap)
      }
      case "removed" => {
        removePatient(messageMap)
      }
    }
  }

  def removePatient(patient: Map[String, Any]) {
    println("TODO: implement removePatient()")
  }

  def postEntirePatientToElastic(patient: Map[String, Any]) {
    println("postEntirePatientToElastic was called")
    val patientId = jsonExtractor(patient, List("data", "patient", "CareContactId"))
    println("patientid: "+ patientId)
    if(!patientId.isInstanceOf[Double]) {
      throw new NoSuchElementException
    }else{
      // create an empty field in elasticsearch for subsequent updates
      val patientWithUpdateHistory = write(Map("data" -> patient.get("data"), "updates"->List)) //TODO fix this. updates is not actually a map

      client.index(
        index = "currentpatients",
        `type` = "currentpatient",
        id = Some(patientId.toString),
        data = patientWithUpdateHistory, //write generates a json string from Map (using json4s)
        refresh = true
      )
    }
  }

  def diffPatient(diff: Map[String, Any]) {

    // figure out the ID of the patient
    val patientId = jsonExtractor(diff, List("data", "updates", "CareContactId"))
    println("patientid: "+ patientId)
    if(!patientId.isInstanceOf[Double]) {
      throw new NoSuchElementException
    }

    // retrieve the patient from elastic
    val oldPatientQuery = client.get("currentpatients", "currentpatient", patientId.toString ).map(_.getResponseBody) //fetch patient from database
    while(!oldPatientQuery.isCompleted){} // the worst possible way to wait for a response from the database
    println("i asked for patient " + patientId+ " and received response:  \n    "+oldPatientQuery.value)
    val oldPatient = jsonToMap(oldPatientQuery.value.get.get) // indeed

    //check if this patient is already in the database
    val alreadyExists = jsonExtractor(oldPatient, List("found")) // the search query contains the field "found" at the top level, very convenient
    if( alreadyExists == true ) {
      println("    i have decided it was not a new patient. adding data to patient...")
      val updatedPatient = applyEventToPatient(oldPatient, diff)
      val updatedData = write(updatedPatient)
      //TODO make sure this actually overwrites the previous instance
      client.index(
        index = "currentpatients",
        `type` = "currentpatient",
        id = Some(patientId.toString),
        data = updatedData,
        refresh = true
      )
    }else{
      println("received an event for a patient that did not exist! this should not happen!")
    }
  }

  def applyEventToPatient(patient: Map[String, Any], update: Map[String, Any] ): Map[String, Any] = {
    val newUpdate =     jsonExtractor(update, List("data", "updates")).asInstanceOf[Map[String, Any]]
    val newEvents =     jsonExtractor(update, List("data", "newEvents")).asInstanceOf[List[Map[String, Any]]]
    val removedEvents = jsonExtractor(update, List("data", "removedEvents")).asInstanceOf[List[Map[String, Any]]]

    println(newUpdate + "\n" + newEvents + "\n" + removedEvents)

    val oldUpdates =    jsonExtractor(patient, List("_source", "updates")).asInstanceOf[List[Map[String, Any]]]
    val oldEvents  =    jsonExtractor(patient, List("_source", "data", "Events")).asInstanceOf[List[Map[String, Any]]]

    val nextEvents  = newEvents  ++ oldEvents    //TODO handle removedevents (probably with .filterNot)
    val nextUpdates = newUpdate ++ oldUpdates

    Map[String, Any](
      "CareContactId" ->                  patient.get("CareContactId"), //should never change
      "CareContactRegistrationTime" ->    newUpdate.getOrElse("CareContactRegistrationTime", patient.get("CareContactRegistrationTime")),
      "DepartmentComment" ->              newUpdate.getOrElse("DepartmentComment", patient.get("DepartmentComment")),
      "Location" ->                       newUpdate.getOrElse("Location", patient.get("Location")),
      "PatientId" ->                      patient.get("PatientId"), //should never change
      "ReasonForVisit" ->                 newUpdate.getOrElse("ReasonForVisit", patient.get("ReasonForVisit")),
      "Team" ->                           newUpdate.getOrElse("Team", patient.get("Team")),
      "VisitId" ->                        newUpdate.getOrElse("VisitId", patient.get("VisitId")),
      "VisitRegistrationTime" ->          newUpdate.getOrElse("VisitRegistrationTime", patient.get("VisitRegistrationTime")),
      "timestamp" ->                      Some(Extraction.decompose(getNow)),

      "Events" -> nextEvents,
      "updates" -> nextUpdates
    )
  }

  // attempts to parse a json String to a Scala map.
  def jsonToMap (json: String): Map[String, Any] = {
    JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
    //TODO proper guards
  }


  /*
  * somewhat ugly method for dealing with supernested json maps. Calling it with {"data", "patient", "value"} will return the object data.patient.value. value can here be a value but may also be a map
  *
  * @param map the top level map
  * @param keys list of map keys
  */
  def jsonExtractor(data: Any, keys: List[String]): Any =  {
    //TODO throw error if first data is not a Map[String, Any]

    if (keys.isEmpty) {
      data
    }else {
      data match {
        //happens if the previous layer did not contain the previous key
        case None => {
          None
        }
        // this means we are not at the top level of the maps; searching continues
        case data:Map[String, Any] => {
          val map = data.asInstanceOf[Map[String, Any]]
          val nextObject = map.getOrElse( keys.head, None )

          //check if object was found
          if(nextObject.equals(None)){
            println("jsonExtractor failed to find object. I was trying to look for: " +keys.toString()+". I was looking for it in: " +map.toString())
          }else{
            jsonExtractor( nextObject, keys.tail )
          }
        }
        case _ => {
          println("jsonExtractor failed to find object. I have reached to bottom layer but still have keys left: " + keys.toString())
          None
        }
      }
    }
  }

  def getNow = {
    DateTime.now(DateTimeZone.forID("Europe/Stockholm"))
  }
}
