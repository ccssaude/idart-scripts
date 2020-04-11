sql_dups_nids <- " select  patientid from patient where patientid in (  select patientid from patient   group by patientid having count(*)>1) "

sql_clinic_name   <- "select   clinicname from public.clinic"
sql_facility_name <- "select facilityname from public.nationalclinics"


sql_regimes <- "SELECT * from public.regimeterapeutico "

sql_update_querys <- 
  c("ALTER TABLE public.prescription ADD COLUMN tipods VARCHAR(255);",
    "ALTER TABLE public.prescription ADD COLUMN dispensasemestral INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE public.prescription ADD COLUMN ccr character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN gaac character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN af character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN ca character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN fr character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN cpn character(1) DEFAULT 'F'",
    "ALTER TABLE public.prescription ADD COLUMN dc character(1) DEFAULT 'F';",
    "ALTER TABLE public.prescription ADD COLUMN saaj character(1) DEFAULT 'F';",
    "ALTER TABLE public.regimeterapeutico ADD COLUMN regimenomeespecificado character varying(60);",
    "ALTER TABLE public.regimeterapeutico ADD COLUMN codigoregime integer ;",
    "update  public.patient  set  uuidopenmrs = uuid;")

sql_create_tbl_query <- "CREATE TABLE public.openmrserrorlog
(
  id integer NOT NULL,
  datecreated timestamp without time zone,
  errordescription character varying(255),
  patient integer,
  pickupdate timestamp without time zone,
  prescription integer,
  returnpickupdate timestamp without time zone,
  CONSTRAINT openmrserrorlog_pkey PRIMARY KEY (id)
) WITH (  OIDS=FALSE);"
  
  
sql_update_nid_pi <- "update public.patientidentifier as pi
set value = p.patientid
from public.patient p
where pi.patient_id=p.id and p.patientid not in 
(
select  patientid
from patient where patientid in (
select patientid from patient 
group by patientid having count(*)>1
))"



