## OnGoingPatients
This service keeps track of the patients currently in the emergency room and saves their data as they leave. It listens
to topic `elvisDiff` and applies the diffs to the patients in an `elasticsearch`. This includes the raw data from the
Elvis clients but also a few analysed fields, mainly time measurements.

Patients that are currently in the emergency room will be indexed under
`/on_going_patients_index/patient_type/CareContactId`, where `CareContactId` is the anonymised id stored in the patient.
Patients that are removed from Elvis are moved to `/finished_patients_index/patient_type/CareContactId`

To work it needs to be paired with a running instance of `Transformationservice`.

# Startup sequence
Make sure `application.conf` is setup correctly. It needs to point to a proper `activemq` and `elasticsearch`.

Now run `OnGoingPatients`. When it reports that it is connected to the bus, run `Transformationservice`. If it is
already running, restart it. On startup it will output all the current patients as `elvisDiff` of type `newLoad`, which
are sent to the database by OnGoingPatients. Note that any patient loaded through `newLoad` will be missing the
history of any data that was changed before the load.