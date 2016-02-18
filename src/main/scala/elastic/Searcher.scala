package elastic

import com.typesafe.config.ConfigFactory
import wabisabi._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.parsing.json.JSON

import scala.concurrent.ExecutionContext.Implicits.global

class Searcher {
  val config = ConfigFactory.load()
  val ip = config.getString("elastic.ip")
  val port = config.getString("elastic.port")

  val client = new Client(s"http://$ip:$port")
  println("Searcher instance connecting to " + ip + ":" + port )


  def postPatientToElastic(body: String) {
    //TODO create this method and make Patient use it
  }

  def postEventToElastic(event: String) {
    println("\nSearcher received a data")

    val eventMap = jsonToMap(event)
    println("data: "+ eventMap.get("data")) // this should output Some(*unreadable json*)

    // figure out the ID of the patient
    val patientId = jsonExtractor(eventMap, List("data", "patient", "CareContactId"))
    println("patientid: "+ patientId)
    if(!patientId.isInstanceOf[Double]) {
      // at the moment this will ALWAYS be thrown by incoming 'updates'.
      // 'updates' keeps it in "updates", "CareContactId"
      throw new NoSuchElementException
    }

    // retrieve the patient from elastic
    val oldPatientQuery = client.get("currentpatients", "currentpatient", patientId.toString ).map(_.getResponseBody) //fetch patient from database
    while(!oldPatientQuery.isCompleted){} // the worst possible way to wait for a response from the database
    println("i asked for patient " + patientId+ " and received response:  \n    "+oldPatientQuery.value)

    val oldPatient = jsonToMap(oldPatientQuery.value.get.get) // indeed
    val alreadyExists = jsonExtractor(oldPatient, List("found")) // the search query contains the field "found" at the top level, very convenient
    if( alreadyExists == true ) {
      println("    i have decided it was not a new patient. adding data to patient...")
      val updatedPatient = applyEventToPatient(oldPatient, eventMap)
      val updatedData = mapToJson(updatedPatient)

      //TODO make sure this actually overwrites the previous instance
      client.index(
        index = "currentpatients",
        `type` = "currentpatient",
        id = Some(patientId.toString),
        data = updatedData,
        refresh = true
      )
    }else{
      println("    it seems this was a new patient. sending it to database...")
      client.index(
        index = "currentpatients",
        `type` = "currentpatient",
        id = Some(patientId.toString),
        data = event,
        refresh = true
      )
    }
  }

  def applyEventToPatient(patient: Map[String, Any], event: Map[String, Any] ): Map[String, Any] = {
    // we just have to mash two maps together, should not be a problem
    event
  }

  def mapToJson (map: Map[String, Any]): String ={
    "TODO: create this method"
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
          println("jsonExtractor failed to find object. I was going to look for: " + keys.toString())
          None
        }
        // this means we are not at the top level of the maps; searching continues
        case data:Map[String, Any] => {
          val map = data.asInstanceOf[Map[String, Any]]
          val nextObject = map.getOrElse( keys.head, None )
            //TODO null chech here. if it is null print keys.head so we can see what happened
          jsonExtractor( nextObject, keys.tail )
        }
        case _ => {
          println("jsonExtractor failed to find object. I have reached to bottom layer but still have keys left: " + keys.toString())
          None
        }
      }
    }
  }
}
