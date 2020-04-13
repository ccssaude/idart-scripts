
sql_clinic_name   <- "select   clinicname from public.clinic"
sql_facility_name <- "select facilityname from public.nationalclinics"
sql_regimes <- "SELECT * from public.regimeterapeutico "
sql_desactiva_regimes_sem_uuid <- "update public.regimeterapeutico set active =FALSE where regimenomeespecificado ='';   "
sql_update_uuid_openmrs <- "update  public.patient  set  uuidopenmrs = uuid;"
sql_update_cod_regime_null <- "UPDATE public.regimeterapeutico SET  codigoregime='1'  WHERE codigoregime is null and active =TRUE ;"

sql_update_nid_patient_identifier <- "update public.patientidentifier as pi
set value = p.patientid
from public.patient p
where pi.patient_id=p.id and p.patientid not in 
(
select  patientid
from patient where patientid in (
select patientid from patient 
group by patientid having count(*)>1
))"



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
) WITH (  OIDS=FALSE); "
  
  

