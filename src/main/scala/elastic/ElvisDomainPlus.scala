package elastic

import com.github.nscala_time.time.Imports._
import org.json4s._

case class ElvisPatient(CareContactId: BigInt,
                        CareContactRegistrationTime: DateTime,
                        DepartmentComment: String,
                        Location: String,
                        PatientId: BigInt,
                        ReasonForVisit: String,
                        Team: String,
                        VisitId: BigInt,
                        VisitRegistrationTime: DateTime,
                        Events: List[ElvisEvent],
                        Updates: List[ElvisUpdateEvent]
                       )



case class ElvisEvent(CareEventId: BigInt,
                      Category: String,
                      End: DateTime,
                      Start: DateTime,
                      Title: String,
                      Type: String,
                      Value: String,
                      VisitId: BigInt
                     )

case class ElvisUpdateEvent(CareContactId: BigInt,
                       PatientId: BigInt,
                       VisitId: BigInt,
                       Timestamp: DateTime  //not shown: actual update. [location, team, etc]
                      )



case class ElvisPatientDiff(updates: Map[String, JValue], newEvents: List[ElvisEvent], removedEvents: List[ElvisEvent])
case class NewPatient(timestamp: DateTime, patient: ElvisPatient)
case class RemovedPatient(timestamp: DateTime, patient: ElvisPatient)
case class SnapShot(patients: List[ElvisPatient])
