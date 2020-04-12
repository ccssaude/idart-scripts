getAllPatientsIdart <- function(connection) {
  patients  <-
    dbGetQuery(
      connection,
      paste0(
        "select pat.id, pat.patientid,dateofbirth::TIMESTAMP::DATE as dateofbirth,lower(pat.firstnames) as firstnames , 
        pat.sex, lower(pat.lastname) as lastname ,pat.uuid,pat.uuidopenmrs, ep.startreason,
        dispensas.total as totalDispensas
        from patient pat left join
        (
         select patient, max(startdate), startreason
           from episode
            group by patient, startreason

        )  ep on ep.patient = pat.id

        left join (
            select patientid, count(*) as total
            from packagedruginfotmp
            group by patientid
       ) dispensas on dispensas.patientid = pat.patientid;


 "
      )
    )
  
  patients  # same as return(patients)
  
}

getAllPatientsOpenMRS <- function(connection) {
  rs  <-
    dbSendQuery(
      connection,
      paste0(
        "

      SELECT   
        pat.patient_id, 
        pid.identifier , 
        pe.uuid,
        lower(pn.given_name) given_name ,
        lower(pn.middle_name) middle_name,
        lower(pn.family_name) family_name,
        concat(lower(pn.given_name),' ' ,if(lower(pn.middle_name) is not null,concat( lower(pn.middle_name), ' ') ,' '),lower(pn.family_name)) as full_name_openmrs ,
        pe.birthdate,
        estado.estado as estado_tarv ,
        max(estado.start_date) data_estado,
        date(visita.encounter_datetime) as data_ult_levant,
        date(visita.value_datetime) as data_prox_marcado
        FROM  patient pat INNER JOIN  patient_identifier pid ON pat.patient_id =pid.patient_id
        INNER JOIN person pe ON pat.patient_id=pe.person_id
        INNER JOIN person_name pn ON pe.person_id=pn.person_id and    pn.voided=0 and pid.preferred=1
        LEFT JOIN
      		(
      			SELECT 	pg.patient_id,ps.start_date encounter_datetime,location_id,ps.start_date,ps.end_date,
      					CASE ps.state
                              WHEN 6 THEN 'ACTIVO NO PROGRAMA'
      						WHEN 7 THEN 'TRANSFERIDO PARA'
      						WHEN 8 THEN 'SUSPENSO'
      						WHEN 9 THEN 'ABANDONO'
      						WHEN 10 THEN 'OBITO'
                              WHEN 29 THEN 'TRANSFERIDO DE'
      					ELSE 'OUTRO' END AS estado
      			FROM 	patient p
      					INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
      					INNER JOIN patient_state ps ON pg.patient_program_id=ps.patient_program_id
      			WHERE 	pg.voided=0 AND ps.voided=0 AND p.voided=0 AND
      					pg.program_id=2 AND ps.state IN (6,7,8,9,10,29) AND ps.end_date IS NULL
      
      
      		) estado ON estado.patient_id=pe.person_id
       LEFT Join
           (
      		Select ult_levantamento.patient_id,ult_levantamento.encounter_datetime,o.value_datetime
      		from
      
      			(	select 	p.patient_id,max(encounter_datetime) as encounter_datetime
      				from 	encounter e
      						inner join patient p on p.patient_id=e.patient_id
      				where 	e.voided=0 and p.voided=0 and e.encounter_type=18
      				group by p.patient_id
      			) ult_levantamento
      			inner join encounter e on e.patient_id=ult_levantamento.patient_id
      			inner join obs o on o.encounter_id=e.encounter_id
      			where o.concept_id=5096 and o.voided=0 and e.encounter_datetime=ult_levantamento.encounter_datetime and
      			e.encounter_type =18
      		) visita  on visita.patient_id=pn.person_id
      
          group by pat.patient_id order by     pat.patient_id
      
              "
            )
    )
  
  data <- fetch(rs, n = -1)
  RMySQL::dbClearResult(rs)
  return(data)
  
}



## Total de levantamentos de cada paciente iDART
getTotalDeDispensas  <- function(connection) {
  totalDispensas  <-
    dbGetQuery(
      connection,
      paste0(
        " select pat.id, dispensas.patientid, dispensas.total as totalDispensas
from patient pat
left join (
 select patientid, count(*) as total
  from packagedruginfotmp
 group by patientid
 ) dispensas on dispensas.patientid = pat.patientid"
      )
    )
  
  totalDispensas # the same as return(totalDispensas)
  
}

getTotalDeDispensasPorPaciente  <- function(patient.id) {
  levPaciente  <-
    dispensasPorPaciente[which(dispensasPorPaciente$id == patient.id), ]
  
  levPaciente$totaldispensas
}



###  Funcoes para actualizar dados na BD iDART

updateUUID <- function (patient.id, uuid) {
  dbExecute(
    con_postgres,
    paste0(
      "update  public.patient set uuid='",
      uuid,
      "' , openmrsuuid='",
      uuis ,
      "'  where id = ",
      id,
      " ;"
    )
  )
  
}




##  Preenche a coluna full_name da tabela/df dos pacientes semm UUID no iDART
## Usaremos essa coluna para buscar simetrias de nomes iDART/OpenMRS

fillFullName <- function (patientsWoutUuid) {
  df <- patientsWoutUuid
  
  df$midle_name <- ""
  df$full_name <- ""
  df$nome_aprox_openmrs <- ""
  df$nid_nome_aprox_openmrs <- ""
  df$birthdate_openmrs <- ""
  df$dist_de_aproximacao <- ""
  df$uuid_openmrs <- ""
  df$estado_tarv_openmrs <- ""
  df$observacao <- ""
  
  for (i in 1:dim(df)[1]) {
    splited_firstnames <-
      strsplit(
        df$firstnames[i],
        split = " ",
        fixed = FALSE,
        perl = FALSE,
        useBytes = FALSE
      )
    first_name <- splited_firstnames[[1]][1]
    midle_name <- splited_firstnames[[1]][2]
    if (length(splited_firstnames[[1]] == 2)) {
      # tem nomes compostos first_name e given_name
      df$firstnames[i] <- first_name
      df$midle_name[i] <- midle_name
      if (!is.na(midle_name)) {
        df$full_name[i] <-
          paste0(first_name, " ", midle_name, " ", df$lastname[i])
      }
      else {
        df$full_name[i] <- paste0(first_name, " ", df$lastname[i])
      }
      
    }
    else{
      df$full_name[i] <- paste0(df$firstnames[i], ''  , df$lastname[i])
    }
    
    
  }
  
  df[, c(
    "id",
    "patientid",
    "dateofbirth",
    "uuid",
    "uuidopenmrs",
    "totalLevantamentos",
    "full_name",
    "nome_aprox_openmrs",
    "nid_nome_aprox_openmrs",
    "birthdate_openmrs",
    "uuid_openmrs",
    "estado_tarv_openmrs",
    "dist_de_aproximacao",
    "observacao"
  )] # o mesmo que return(df)
}
