package elastic

import com.github.nscala_time.time.Imports._
import org.json4s._

//TODO decide whether or not Priority should be added as a field
case class ElvisPatientPlus(
  CareContactId: BigInt,
  CareContactRegistrationTime: DateTime,
  DepartmentComment: String,
  Location: String,
  PatientId: BigInt,
  ReasonForVisit: String,
  Team: String,
  VisitId: BigInt,
  VisitRegistrationTime: DateTime,
  //Priority: String              // Commented because it might be a bad idea. Priority is also an ElvisEvent type
  Events: List[ElvisEvent],
  Updates: List[ElvisUpdateEvent] // this is the plus part
)

case class ElvisEvent(
  CareEventId: BigInt,
  Category: String,
  End: DateTime,
  Start: DateTime,
  Title: String,
  Type: String,
  Value: String,
  VisitId: BigInt
)

/** Analogous for ElvisEvent, but for text field changes */
case class ElvisUpdateEvent(
  CareContactId: BigInt,
  PatientId: BigInt,
  VisitId: BigInt,
  Timestamp: DateTime ,
  ModifiedField: String,
  ModifiedTo : JValue
)

case class ElvisPatientDiff(updates: Map[String, JValue], newEvents: List[ElvisEvent], removedEvents: List[ElvisEvent])
case class NewPatient(timestamp: DateTime, patient: ElvisPatientPlus)
case class RemovedPatient(timestamp: DateTime, patient: ElvisPatientPlus)
case class SnapShot(patients: List[ElvisPatientPlus])
