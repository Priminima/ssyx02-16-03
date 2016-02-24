package elastic

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import wabisabi._
import org.json4s.native.JsonMethods._
import org.json4s._
import org.json4s.native.Serialization.{read, write}
import scala.concurrent.ExecutionContext.Implicits.global

class PatientsToElastic {
  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all // json4s needs this for something

  // Used for indexing to and from elasticsearch.
  // active patients are stored in ONGOING_PATIENT_INDEX/PATIENT_TYPE/CareContactId
  // removed patients are stored in FINISHED_PATIENT_INDEX/PATIENT_TYPE/CareContactId
  val PATIENT_TYPE = "patient_type"
  val ONGOING_PATIENT_INDEX= "on_going_patient_index"
  val FINISHED_PATIENT_INDEX = "finished_patient_index"   // patients go here when they are removed

  // load configs from resources/application.conf
  val config = ConfigFactory.load()
  val ip = config.getString("elastic.ip")
  val port = config.getString("elastic.port")

  println("PatientsToElastic instance attempting to connect to elasticsearch on address: " + ip + ":" + port )
  val client = new Client(s"http://$ip:$port") // creates a wabisabi client for communication with elasticsearch

  /** Handles the initial parsing of incoming messages */
  def messageReceived(message: String) {
    println("*************************new message*************************") // very nice
    println("time: "+getNow+ " message: "+ message)
    // figure out what sort of message we just received
    val json = parse(message) // this jsons the String.
    val isa = json \ "isa"
    println("isa: " + isa)

    isa match {
      case JString("newLoad") | JString("new") => postEntirePatientToElastic(message)
      case JString("diff")                     => diffPatient(message)
      case JString("removed")                  => removePatient(message)
      case _ => println("WARNING: Searcher received an unrecognized message format. isa:"+isa)
    }
  }

  def postEntirePatientToElastic(patientString: String): Unit = {
    val json:JValue = parse(patientString) // generate a json map
    val patient:String = write(json \ "data" \ "patient") // dig out the good stuff from the json map
    val elvisPatient:ElvisPatientPlus = read[ElvisPatientPlus](patient) //create an ElvisPatient from the good stuff

    addPatient(elvisPatient, ONGOING_PATIENT_INDEX) // index the new patient
  }

  /** This extensive method applies a diff to an OnGoingPatient.
    *
    * The incoming diff is parsed and the CareContactId of the diff is extracted. the relevant patient is fetched from
    * the database using the CareContactId. The next iteration of the patient is generated from the diff and the old
    * patient. The new patient is then indexed into the database, overwritning the old version
    *
    *
    *
    * @param diffString a json string describing the patient diff. It should be a String with "isa"->"diff"
    */
  def diffPatient(diffString: String) {
    // parse the string to an ElvisPatientDiff
    val json:JValue = parse(diffString)
    val diff:String = write(json \ "data")
    val elvisDiff: ElvisPatientDiff  = read[ElvisPatientDiff](diff)

    // extract CareContactId (needed to fetch patient from elasticsearch)
    val careContactId:String = elvisDiff.updates.get("CareContactId").get.values.toString // updates is still a map, which gives us this interesting line
    println("CareContactId: " + careContactId)

    // retrieve the patient from elastic
    val oldPatientQuery = client.get(ONGOING_PATIENT_INDEX, PATIENT_TYPE, careContactId).map(_.getResponseBody) //fetch patient from database
    while (!oldPatientQuery.isCompleted) {} // patiently wait for response from the database. //TODO at some point add timeout. It could get stuck here forever (but probably not)
    val oldPatient:JValue = parse(oldPatientQuery.value.get.get) // unpack the string and cast to json-map
    val oldPatientData:String = write(oldPatient \ "_source") // The good stuff is located in _source. Turn the good stuff back into a String
    val elvisPatient:ElvisPatientPlus  = read[ElvisPatientPlus](oldPatientData) // finally, parse the good stuff to an ElvisPatientPlus
    println("i asked for patient " + careContactId + " and received response:  \n    " + elvisPatient.toString)

    // add and remove Events from event array
    val actualNewEvents = elvisDiff.newEvents.filter(e => !elvisPatient.Events.contains(e))       // filter out Events from newEvents (do not add an Event that has already happened)
    val actualOldEvents = elvisPatient.Events.filter(e => !elvisDiff.removedEvents.contains(e))   // filter out removedEvents from Events
    val eventList = actualOldEvents ++ actualNewEvents

    // create elvisUpdateEvents
    val updateList:List[ElvisUpdateEvent] = createNewUpdateList(elvisPatient, elvisDiff.updates)

    // now rebuild the patient
    val newPatient = new ElvisPatientPlus(
      // if updates.get("foo") exists, use that. Otherwise use the old value of foo.
      CareContactId =               updateOrElse( elvisDiff.updates.get("CareContactId"),                elvisPatient.CareContactId),
      CareContactRegistrationTime = updateOrElse( elvisDiff.updates.get("CareContactRegistrationTime"),  elvisPatient.CareContactRegistrationTime),
      DepartmentComment =           updateOrElse( elvisDiff.updates.get("DepartmentComment"),            elvisPatient.DepartmentComment),
      Location =                    updateOrElse( elvisDiff.updates.get("Location"),                     elvisPatient.Location),
      PatientId =                   updateOrElse( elvisDiff.updates.get("PatientId"),                    elvisPatient.PatientId),
      ReasonForVisit =              updateOrElse( elvisDiff.updates.get("ReasonForVisit"),               elvisPatient.ReasonForVisit),
      Team =                        updateOrElse( elvisDiff.updates.get("Team"),                         elvisPatient.Team),
      VisitId =                     updateOrElse( elvisDiff.updates.get("VisitId"),                      elvisPatient.VisitId),
      VisitRegistrationTime =       updateOrElse( elvisDiff.updates.get("VisitRegistrationTime"),        elvisPatient.VisitRegistrationTime), // i should make a script to automate copying these lines
      Events =                      eventList,
      Updates =                     updateList
    )
    // finally, index the updated patient
    addPatient(newPatient, ONGOING_PATIENT_INDEX)
  }

  /** This horrible mess generates for a patient a new list of ElvisUpdateEvent, given a Map of updates.
    * @param patient the patient to apply the updates to. The old list of ElvisUpdateEvent is retrieved from this patient
    * @param newUpdates a map of the new updates
    * @return new ElvisUpdateEvent list
    */
  def createNewUpdateList(patient: ElvisPatientPlus, newUpdates: Map[String, JValue] ): List[ElvisUpdateEvent]= {
    val relevantKeys = List("DepartmentComment", "Location", "ReasonForVisit", "Team") // the four keys which interest us
    var updatedVariables: List[ElvisUpdateEvent] = patient.Updates  // make a mutable list and fill it with the current list of updates

    relevantKeys.foreach( k => if(newUpdates.getOrElse(k, JNothing) != JNothing){ // foreach relevant key, check if newUpdates has an associated value
      println("update recorded: " +k+ "->" +newUpdates.get(k).get.values)
      updatedVariables ++= List(
        read[ElvisUpdateEvent](write(Map( // ElvisUpdateEvent made from String made from Map
          "CareContactId"-> patient.CareContactId,
          "VisitId" -> patient.VisitId,
          "PatientId" -> patient.PatientId,
          "Timestamp"-> newUpdates.get("timestamp").get.values, // please note timestamp vs Timestamp
          "ModifiedField" -> k,                         // the key for the field that was changed...
          "ModifiedTo" -> newUpdates.get(k).get.values  // ...and the value it was changed to
        )))).filter(k => !updatedVariables.contains(k)) // filter out any update that has already been recorded. This filter is called on a list that contains one element.
    })
    updatedVariables
  }

  /** if update is a an JValue, return update.asInstanceOf[A]. Otherwise (if update is None), old is returned. */
  def updateOrElse[A](update: Any, old: A): A = {
    println("update: "+ update +"    "+ old)
    update match {
      case Some(a:JValue) => {
        update.asInstanceOf[Option[JValue]].get.values.asInstanceOf[A]
      }
      case _ => {
        old
      }
    }
  }

  /** Deletes a patient from ONGOING_PATIENT_INDEX and adds it to FINISHED_PATIENT_INDEX
    * @param patientString the patient to be removed [JSON]
    */
  def removePatient(patientString: String) {
    val json:JValue = parse(patientString) // gererate a json map
    val patient:String = write(json \ "data" \ "patient") // dig out the good stuff from the json map
    val elvisPatient:ElvisPatientPlus = read[ElvisPatientPlus](patient) //create an ElvisPatient from the data

    // delete patient from ongoing and send to finished patients
    deletePatient(elvisPatient, ONGOING_PATIENT_INDEX)
    addPatient(elvisPatient, FINISHED_PATIENT_INDEX)
  }

  /** Attempts to delete a patient from elasticsearch under /targetIndex/PATIENT_TYPE/patient.CareContactId
    * @param patient the patient to be removed, in the ElvisPatientPlus format
    * @param targetIndex should ONLY be one of the values ONGOING_PATIENT_INDEX or FINISHED_PATIENT_INDEX
    */
  def deletePatient(patient : ElvisPatientPlus, targetIndex: String): Unit ={
    val careContactId:String = patient.CareContactId.toString()
    client.delete(
      index = targetIndex,
      `type` = PATIENT_TYPE,
      id = careContactId
    )
  }

  /** Adds a patient to elasticsearch under /targetIndex/PATIENT_TYPE/patient.CareContactId
    * Note that this overwrites anything previously on that path
    * @param patient the patient to be added, in the ElvisPatientPlus format
    * @param targetIndex should ONLY be one of the values ONGOING_PATIENT_INDEX or FINISHED_PATIENT_INDEX
    */
  def addPatient(patient : ElvisPatientPlus, targetIndex: String): Unit = {
    val careContactId:String = patient.CareContactId.toString()
    client.index(
      index = targetIndex,
      `type` = PATIENT_TYPE,
      id = Some(careContactId),
      data = write(patient),
      refresh = true
    )
  }
  
  def getNow = {
    DateTime.now(DateTimeZone.forID("Europe/Stockholm"))
  }
}

