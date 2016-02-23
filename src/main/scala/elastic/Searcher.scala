package elastic

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import elastic.ElvisPatientPlus


import wabisabi._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.parsing.json.JSON
import org.json4s.native.JsonMethods._
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import scala.concurrent.ExecutionContext.Implicits.global

class Searcher {
  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all // for json serialization
  val INDEX = "patients"
  val ON_GOING_PATIENT_TYPE = "OnGoingPatient"
  val FINISHED_PATIENT_TYPE = "FinishedPatient"

  val config = ConfigFactory.load()
  val ip = config.getString("elastic.ip")
  val port = config.getString("elastic.port")

  val client = new Client(s"http://$ip:$port")
  println("Searcher instance connecting to " + ip + ":" + port )


  def postJsonToElastic(message: String) {
    println("\nSearcher received a message!")
    println("MESSAGE: "+ message)

    val json = parse(message)
    val isa = json \ "isa"
    println("isa: " + isa)

    isa match {
      case JString("newLoad") => {
        postEntirePatientToElastic(message)
      }
      case JString("new") => {
        postEntirePatientToElastic(message)
      }
      case JString("diff") => {
        diffPatient(message)
      }
      case JString("removed") => {
        removePatient(message)
      }
      case _ => {
        println("WARNING: Searcher received an unrecognized message format. isa:"+isa)
      }
    }
  }

  def removePatient(patient: String) {
    println("TODO: implement removePatient()")
  }

  def postEntirePatientToElastic(patientString: String): Unit = {
    println("postEntirePatientToElastic was called")

    val json = parse(patientString)
    val patient = write(json \ "data" \ "patient") // dig out the good stuff from the json map
    val elvisPatient:ElvisPatientPlus = read[ElvisPatientPlus](patient) //create an ElvisPatient from the data

    // dig up the careContactId to use as elasticsearch index
    val careContactId = elvisPatient.CareContactId.toString
    println("CareContactId: " + careContactId)

    client.index(
      index = INDEX,
      `type` = ON_GOING_PATIENT_TYPE,
      id = Some(careContactId),
      data = write(elvisPatient),
      refresh = true
    )
  }

  def diffPatient(diffString: String) {
    println("diffPatient was called")
    val json = parse(diffString)
    val diff = write(json \ "data") //if it is not a proper diff this part will maybe not fail. That is not a good thing
    //println("I parsed the diff like so: " + diff)
    val elvisDiff: ElvisPatientDiff  = read[ElvisPatientDiff](diff)
    //println("I casted it to an ElvisPatientDiff so: " + diff)
    val careContactId = elvisDiff.updates.get("CareContactId").get.values.toString // this is insane
    println("CareContactId: " + careContactId)

    // retrieve the patient from elastic
    val oldPatientQuery = client.get(INDEX, ON_GOING_PATIENT_TYPE, careContactId).map(_.getResponseBody) //fetch patient from database
    while (!oldPatientQuery.isCompleted) {} // the worst possible way to wait for a response from the database. //TODO add timeout (like a coward)
    println("i asked for patient " + careContactId + " and received response:  \n    " + oldPatientQuery.value)
    val oldPatient = parse(oldPatientQuery.value.get.get) // wow.
   // println("oldPatient: " + oldPatient)
    val elvisPatient:ElvisPatientPlus  = read[ElvisPatientPlus](write(oldPatient \ "_source")) // finally, parse to ElvisPatient. The good stuff is located in _source. Hopefully no one will see i called read(write())

    // add and remove events from event array
    val actualNewEvents = elvisDiff.newEvents.filter(e => !elvisPatient.Events.contains(e))       // filter out Events from newEvents
    val actualOldEvents = elvisPatient.Events.filter(e => !elvisDiff.removedEvents.contains(e))   // filter out removedEvents from Events
    val events : List[ElvisEvent] = actualNewEvents ++ actualOldEvents                            // add newEvents to list of Events

    //println("Events: " + events)
    println(  "SUPER DEBUG GO")
    println(  elvisDiff.newEvents)
    println(  elvisPatient.Events)
    println(  elvisDiff.removedEvents)
    println(  actualNewEvents)
    println(  actualOldEvents)
    println(  events)

    // create elvisUpdateEvents
    val updates = updateExtractor(elvisDiff.updates)
    val updateEventList = addUpdatesToList(elvisPatient, updates)

    // create priority
    //val priority = elvisPatient.Priority
    //TODO add priority
    //TODO also check actualNewEvents for a new priority

    // now rebuild the patient
    val newPatient = new ElvisPatientPlus( //TODO cleanse this abomination
      CareContactId =               updateOrElse( updates.getOrElse("CareContactId", Some(None)),                elvisPatient.CareContactId),
      CareContactRegistrationTime = updateOrElse( updates.getOrElse("CareContactRegistrationTime", Some(None)),  elvisPatient.CareContactRegistrationTime),
      DepartmentComment =           updateOrElse( updates.getOrElse("DepartmentComment", Some(None)),            elvisPatient.DepartmentComment),
      Location =                    updateOrElse( updates.getOrElse("Location", Some(None)),                     elvisPatient.Location),
      PatientId =                   updateOrElse( updates.getOrElse("PatientId", Some(None)),                    elvisPatient.PatientId),
      ReasonForVisit =              updateOrElse( updates.getOrElse("ReasonForVisit", Some(None)),               elvisPatient.ReasonForVisit),
      Team =                        updateOrElse( updates.getOrElse("Team", Some(None)),                         elvisPatient.Team),
      VisitId =                     updateOrElse( updates.getOrElse("VisitId", Some(None)),                      elvisPatient.VisitId),
      VisitRegistrationTime =       updateOrElse( updates.getOrElse("VisitRegistrationTime", Some(None)),        elvisPatient.VisitRegistrationTime),


      Events =                      events,
      Updates =                     updateEventList
    )

    client.index(
      index = INDEX,
      `type` = ON_GOING_PATIENT_TYPE,
      id = Some(careContactId),
      data = write(newPatient),
      refresh = true
    )
  }

  def updateExtractor(updates: Map[String, JValue]): Map[String, Option[JValue]] = {
    Map(
      "CareContactId" ->               updates.get("CareContactId"),
      "CareContactRegistrationTime" -> updates.get("CareContactRegistrationTime"),
      "DepartmentComment" ->           updates.get("DepartmentComment"),
      "Location" ->                    updates.get("Location"),
      "PatientId" ->                   updates.get("PatientId"),
      "ReasonForVisit" ->              updates.get("ReasonForVisit"),
      "Team" ->                        updates.get("Team"),
      "VisitId" ->                     updates.get("VisitId"),
      "VisitRegistrationTime" ->       updates.get("VisitRegistrationTime"),
      "Timestamp" ->                   updates.get("timestamp")
    ).filter(nones => nones._2.isDefined)
  }

  def addUpdatesToList(patient: ElvisPatientPlus, news: Map[String, Option[JValue]] ): List[ElvisUpdateEvent]= {
    val relevantKeys = List("DepartmentComment", "Location", "ReasonForVisit", "Team")
    var updatedVariables: List[ElvisUpdateEvent] = patient.Updates
    relevantKeys.foreach( k =>
      if(news.getOrElse(k, JNothing) != JNothing){
        println("update recorded: " +k+ "->" +news.get(k))
        updatedVariables ++= List(
          read[ElvisUpdateEvent](
            write(
              Map(
                "CareContactId"-> patient.CareContactId,
                "VisitId" -> patient.VisitId,
                "PatientId" -> patient.PatientId,
                "Timestamp"-> news.get("Timestamp").get.get.values,
                "ModifiedField" -> k,
                "ModifiedTo" -> news.get(k).get.get.values
              )
            )
          )
        ).filter(k => !updatedVariables.contains(k)) // filter out any update that has already been recorded. This filter is called on a list that contains one element.
      }
    )
    updatedVariables
  }

  def updateOrElse[A](update: Any, old: A): A = { //TODO make update: Option[JValue] and get rid of cast
    update match {
      case Some(None) => {
        old
      }
      case _ => {
        update.asInstanceOf[Option[JValue]].get.values.asInstanceOf[A] // sigh
      }
    }
  }

  def getNow = {
    DateTime.now(DateTimeZone.forID("Europe/Stockholm"))
  }
}


