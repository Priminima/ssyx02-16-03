package elastic

import javax.swing.JList

import com.github.nscala_time.time.Imports._
import com.typesafe.config.ConfigFactory
import wabisabi._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._
import org.json4s._
import org.json4s.JsonDSL._
import scala.concurrent.ExecutionContext.Implicits.global // Client.get gets sad without this

class PatientsToElastic {
  implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all // json4s needs this for something

  // Used for indexing to and from elasticsearch.
  // active patients are stored in ONGOING_PATIENT_INDEX/PATIENT_TYPE/CareContactId
  // removed patients are stored in FINISHED_PATIENT_INDEX/PATIENT_TYPE/CareContactId
  val PATIENT_TYPE = "patient_type"
  val ONGOING_PATIENT_INDEX= "on_going_patient_index"
  val FINISHED_PATIENT_INDEX = "finished_patient_index" // patients go here when they are removed

  // load configs from resources/application.conf
  val config = ConfigFactory.load()
  val ip = config.getString("elastic.ip")
  val port = config.getString("elastic.port")

  println("PatientsToElastic instance attempting to connect to elasticsearch on address: " + ip + ":" + port )
  val client = new Client(s"http://$ip:$port") // creates a wabisabi client for communication with elasticsearch

  /** Handles the initial parsing of incoming messages */
  def messageReceived(message: String) {
    println("************************* new message *************************") // very nice
    println("time: "+getNow+ " message: "+ message)
    // figure out what sort of message we just received
    val json:JValue = parse(message) // this jsons the String.
    val isa = json \ "isa"
    println("isa: " + isa)

    isa match {
      case JString("newLoad") | JString("new") => postEntirePatientToElastic(json)
      case JString("diff")                     => diffPatient(json)
      case JString("removed")                  => removePatient(json)
      case _ => println("WARNING: Searcher received an unrecognized message format. isa:"+isa)
    }
  }

  /** Instantiates a patient and sends it to ONGOING_PATIENT_INDEX
    *
    * @param data patient to send to elastic
    */
  def postEntirePatientToElastic(data: JValue): Unit = {
    val patient = data \ "data" \"patient"
    //   val json:JValue = parse(patientString) // generate a json map
//    val patient:String = write(json \ "data" \ "patient") // dig out the good stuff from the json map
  //  val elvisPatient:ElvisPatientPlus = read[ElvisPatientPlus](patient) //create an ElvisPatient from the good stuff

    //TODO do analytics TTT TTD TTF ETC
    addPatient(patient, ONGOING_PATIENT_INDEX) // index the new patient
  }

  /** Applies a diff to an OnGoingPatient.
    *
    * The incoming diff is parsed and the CareContactId of the diff is extracted. the relevant patient is fetched from
    * the database using the CareContactId. The next iteration of the patient is generated from the diff and the old
    * patient. The new patient is then indexed into the database, overwriting the old version
    */
  def diffPatient(data: JValue) {
    val diff = data \ "data"
    // extract CareContactId and fetch patient from elasticsearch
    val careContactId:String = ( diff \ "updates" \ "CareContactId" ).values.toString // updates is still a map, which gives us this interesting line
    println("CareContactId: " + careContactId)
    val patient:JValue = getPatientFromElastic(ONGOING_PATIENT_INDEX, careContactId)

    // add and remove Events from event array
    val oldEvents:      List[ElvisEvent] = castJValueToList[ElvisEvent](patient \ "Events")
    val newEvents:      List[ElvisEvent] = castJValueToList[ElvisEvent](diff \ "newEvents")
    val removedEvents:  List[ElvisEvent] = castJValueToList[ElvisEvent](diff \ "removedEvents")
    println(oldEvents)
    println(newEvents)
    println(removedEvents)
    println(patient\ "Events")
    println(diff \ "newEvents")
    println(diff \ "removedEvents")

   // val actualNewEvents:List[ElvisEvent] = newEvents.filter(e => !oldEvents.contains(e))       // filter out Events from newEvents (do not add an Event that has already happened)
   // val actualOldEvents:List[ElvisEvent] = oldEvents.filter(e => !removedEvents.contains(e))   // filter out removedEvents from Events
    val actualNewEvents:List[ElvisEvent] = newEvents
    val actualOldEvents:List[ElvisEvent] = oldEvents

    val events:List[ElvisEvent] = actualOldEvents ++ actualNewEvents
    // fuck.

    // create elvisUpdateEvents
    val updates:List[ElvisUpdateEvent] = createNewUpdateList(patient, diff \ "updates")

    // create field Data
    // val fieldData = updateFields(elvisPatient, elvisDiff.updates)
    val fieldData = patient merge (diff \ "updates")

    // now rebuild the patient
    val newPatient = elvisPatientFactory(fieldData, events, updates)

    // finally, index the updated patient
    addPatient(newPatient, ONGOING_PATIENT_INDEX)
  }

  def castJValueToList[A](list:JValue): List[A] = {
    list match {
      case JNothing => List[A]()
      case _ => list.asInstanceOf[JArray].values.asInstanceOf[List[A]]
    }
  }

  def elvisPatientFactory(fieldData: JValue, events: List[ElvisEvent], updates: List[ElvisUpdateEvent]): JValue = {
    fieldData merge
    (
      ("Events" -> parse(write(events))) ~
      ("Updates" ->  parse(write(updates)) )
    )
  }

  private def getTimeToDoctor(events: List[ElvisEvent], visitRegistrationTime: DateTime): Long ={
    events.foreach(e =>
      if(e.Title == "Läkare") { return {
        (visitRegistrationTime to e.Start).toDurationMillis
      }}
    )
    return -1
  }

  private def getTimeToTriage(events: List[ElvisEvent], visitRegistrationTime: DateTime): Long ={
    events.foreach(e =>
      if(e.Title == "Triage") { return {
        visitRegistrationTime.getMillis - e.Start.getMillis
      }}
    )
    return -1
  }

  private def getTotalTime(removedTime: Option[DateTime], visitRegistrationTime: DateTime): Long ={
    removedTime match {
      case None => -1
      case _ => visitRegistrationTime.getMillis - removedTime.get.getMillis
    }
  }

  /** returns the priority event with the latest timestamp */
  val prios = List("Blå","Grön","Gul","Orange","Röd")
  private def getPriority(events: List[ElvisEvent]): String ={
    var timestamp = DateTime.parse("0000-01-24T00:00:00Z")
    var prio:String = ""
    events.foreach( e =>
      if(prios.contains(e.Title)){
        if (e.Start.compareTo(timestamp) > 0) {
          timestamp = e.Start
          prio = e.Title
        }
      }
    )
    prio
  }

  /** This horrible mess generates for a patient a new list of ElvisUpdateEvent, given a Map of updates.
    *
    * @param patient the patient to apply the updates to. The old list of ElvisUpdateEvent is retrieved from this patient
    * @param newUpdates a map of the new updates
    * @return new ElvisUpdateEvent list
    */
  def createNewUpdateList(patient: JValue, newUpdates: JValue ): List[ElvisUpdateEvent]= {
    val relevantKeys = List("DepartmentComment", "Location", "ReasonForVisit", "Team") // the four keys which interest us
    var updatedVariables: List[ElvisUpdateEvent] = castJValueToList[ElvisUpdateEvent](patient \ "updates")  // make a mutable list and fill it with the current list of updates
 //TODO double check if still works
    println("o hai: " + newUpdates)
    relevantKeys.foreach( k => if(newUpdates \ k != JNothing){ // foreach relevant key, check if newUpdates has an associated value

      println("update recorded: " +k+ "->" +(newUpdates \ k).values)
      updatedVariables ++= List(
        read[ElvisUpdateEvent](write(Map( // ElvisUpdateEvent made from String made from Map
          "CareContactId"-> patient \ "CareContactId",
          "VisitId" -> patient\"VisitId",
          "PatientId" -> patient\"PatientId",
          "Timestamp"-> (newUpdates\"timestamp").values, // please note timestamp vs Timestamp
          "ModifiedField" -> k,                         // the key for the field that was changed...
          "ModifiedTo" -> (newUpdates\k).values  // ...and the value it was changed to
        )))).filter(k => !updatedVariables.contains(k)) // filter out any update that has already been recorded. This filter is called on a list that contains one element.
    })

    updatedVariables
  }

  /** if update is a JValue, return update.asInstanceOf[A]. Otherwise (if update is None), old is returned. */
  def updateOrElse[A](update: Any, old: A): A = {
    update match {
      case Some(a:JValue) =>  update.asInstanceOf[Option[JValue]].get.values.asInstanceOf[A]
      case _ =>  old
    }
  }

  /** Deletes a patient from ONGOING_PATIENT_INDEX and adds it to FINISHED_PATIENT_INDEX
    *
    * @param patient the patient to be removed (as a JSON)
    */
  def removePatient(patient: JValue) {
    // create field Data
    /* //TODO set removed time
    val removed = "RemovedTime"->getNow
    val fieldData = modifyFields(patient, removed)
    // now rebuild the patient
    val newPatient = elvisPatientFactory(fieldData, patient \ "Events", patient \ "Updates")
*/
    // delete patient from ongoing and send to finished patients
    deletePatient(patient, ONGOING_PATIENT_INDEX)
    addPatient(patient, FINISHED_PATIENT_INDEX)
  }

  /** Attempts to delete a patient from elasticsearch under /targetIndex/PATIENT_TYPE/patient.CareContactId
    *
    * @param patient the patient to be removed, in the ElvisPatientPlus format
    * @param targetIndex should ONLY be one of the values ONGOING_PATIENT_INDEX or FINISHED_PATIENT_INDEX
    */
  def deletePatient(patient : JValue, targetIndex: String): Unit = {
    val careContactId:String = (patient \ "CareContactId").values.toString
    client.delete(
      index = targetIndex,
      `type` = PATIENT_TYPE,
      id = careContactId
    )
  }

  /** Adds a patient to elasticsearch under /targetIndex/PATIENT_TYPE/patient.CareContactId
    * Note that this overwrites anything previously on that path
    *
    * @param patient the patient to be added, in the ElvisPatientPlus format
    * @param targetIndex should ONLY be one of the values ONGOING_PATIENT_INDEX or FINISHED_PATIENT_INDEX
    */
  def addPatient(patient : JValue, targetIndex: String): Unit = {
    val careContactId:String = (patient \ "CareContactId").values.toString
    client.index(
      index = targetIndex,
      `type` = PATIENT_TYPE,
      id = Some(careContactId),
      data = write(patient),
      refresh = true
    )
  }

  /** Fetches from elastic the patient under /index/PATIENT_TYPE/careContactId  */
  def getPatientFromElastic(index: String, careContactId: String): JValue ={
    val oldPatientQuery = client.get(index, PATIENT_TYPE, careContactId).map(_.getResponseBody) //fetch patient from database
    while (!oldPatientQuery.isCompleted) {} // patiently wait for response from the database. //TODO at some point add timeout. It could get stuck here forever (but probably not)
    val oldPatient:JValue = parse(oldPatientQuery.value.get.get) // unpack the string and cast to json-map

    println("patient gotted: " +oldPatient \ "_source")
    return oldPatient \ "_source" // The good stuff is located in _source.
  }

  def getNow = {
    DateTime.now(DateTimeZone.forID("Europe/Stockholm"))
  }
}
