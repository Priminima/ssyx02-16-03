package elastic

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import elastic.ElvisPatient


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
      // these first two should do the same thing i think
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
    }
  }

  def removePatient(patient: String) {
    println("TODO: implement removePatient()")
  }

  def postEntirePatientToElastic(patientString: String): Unit = {
    println("postEntirePatientToElsatic was called")

    val json = parse(patientString)
    val patient = write(json \ "data" \ "patient") //if it is not a proper newLoad this part will super fail
    println("I parsed the patient data like so: " + patient)
    val elvisPatient:ElvisPatient  = read[ElvisPatient](patient) //TODO feel dumb about not using this at any point in the last three iterations
    println("i casted it to an ElvisPatient like so: " + elvisPatient)
    // dig up the careContactId to use as elasticsearch index
    val careContactId = elvisPatient.CareContactId.toString
    println("CareContactId: " + careContactId)

    client.index(
      index = "currentpatients",
      `type` = "currentpatient",
      id = Some(careContactId),
      data = write(elvisPatient),
      refresh = true
    )
  }

  def diffPatient(diffString: String) {
    println("diffPatient was called")
    val json = parse(diffString)
    val diff = write(json \ "data") //if it is not a proper diff this part will maybe not fail. That is not a good thing
    println("I parsed the diff like so: " + diff)
    val elvisDiff: ElvisPatientDiff  = read[ElvisPatientDiff](diff)
    println("I casted it to an ElvisPatientDiff so: " + diff)
    val careContactId = elvisDiff.updates.get("CareContactId").get.values.toString // this is insane
    println("CareContactId: " + careContactId)

    // retrieve the patient from elastic
    val oldPatientQuery = client.get("currentpatients", "currentpatient", careContactId).map(_.getResponseBody) //fetch patient from database
    while (!oldPatientQuery.isCompleted) {} // the worst possible way to wait for a response from the database. //TODO add timeout (like a coward)
    println("i asked for patient " + careContactId + " and received response:  \n    " + oldPatientQuery.value)
    val oldPatient = parse(oldPatientQuery.value.get.get) // wow.
    println("oldPatient: " + oldPatient)
    val elvisPatient:ElvisPatient  = read[ElvisPatient](write(oldPatient \ "_source")) // finally, parse to ElvisPatient. The good stuff is located in _source. Hopefully no one will see i called read(write())

    // adda nd remove events from event array
    val actualNewEvents = elvisDiff.newEvents.filter(e => elvisPatient.Events.contains(e))       // new events are only new if they are new
    val actualOldEvents = elvisPatient.Events.filter(e => !elvisDiff.removedEvents.contains(e))  // remove events
    val events : List[ElvisEvent] = actualNewEvents ++ actualOldEvents
    println("Events: " + events)

    // create elvisUpdateEvents
    val updates = updateExtractor(elvisDiff.updates)
    val updateEventList = addUpdatesToList(elvisPatient, updates)

    // now rebuild the patient
    val newPatient = new ElvisPatient(
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
    ) // Some(Shame)

    client.index(
      index = "currentpatients",
      `type` = "currentpatient",
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

  def addUpdatesToList(patient: ElvisPatient, news: Map[String, Option[JValue]] ): List[ElvisUpdateEvent]= {
    //TODO block insertion of identical updates

    val relevantKeys = List("DepartmentComment", "Location", "ReasonForVisit", "Team")
    var updatedVariables: List[ElvisUpdateEvent] = patient.Updates
    relevantKeys.foreach( k =>
      if(news.getOrElse(k, JNothing) != JNothing){
        println("HEY LOOK A VALUE WAS CHANGED YOU SHOULD CHECK IF IT WAS ADDED PROPERLY")
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
        ).filter(k => !updatedVariables.contains(k))
      }
    )
    updatedVariables
  }
 // val elvisPatient:ElvisPatient  = read[ElvisPatient](write(oldPatient \ "_source")) // finally, parse to ElvisPatient. The good stuff is located in _source. Hopefully no one will see i called read(write())

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


