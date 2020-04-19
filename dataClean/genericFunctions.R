
#' Busca todos pacientes do OpenMRS
#' 
#' @param con.postgres  obejcto de conexao com BD OpenMRS
#' @return tabela/dataframe/df com todos paciente do OpenMRS 
#' @examples patients_idart <- getAllPatientsIdart(con_openmrs)
getAllPatientsOpenMRS <- function(con.openmrs) {
  rs  <-
    dbSendQuery(
      con.openmrs,
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


#' Busca todos pacientes do iDART
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @return tabela/dataframe/df com total de  lev por paciente
#' @examples total_dispensas <- getTotalDeDispensas(con_idart)
getAllPatientsIdart <- function(con.postgres) {
  patients  <-
    dbGetQuery(
      con.postgres,
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

#' Busca o total de levantamentos de um Paciente no iDART  
#' TODO -  funcao referencia uma variavel global-- deve se optimizar de modo que seja um parametro
#' 
#' @param patient.id id do paciente na tabela  Patient
#' @param uuid.openmrs uuid do  OpenMRS
#' @return 0/1  (0) - error  (1) - sucess   
#' @examples updateUUID(67,  uuid)
getTotalDeDispensasPorPaciente  <- function(patient.id) {
  levPaciente  <-
    dispensasPorPaciente[which(dispensasPorPaciente$id == patient.id), ]
  
  levPaciente$totaldispensas
}


#' Escreve  os logs das accoes executadas nas DBs iDART/OpenMRS numa tabela logsExecucao
#' 
#' @param patient.info informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]   
#' @param action descricao das accoes executadas sobre o paciente
#' @return append uma row na tabela logs
#' @examples
#' logAction(patientToUpdate, ' Paciente com NID X Alrerado Para NID Y')
logAction <- function (patient.info,action){
  
  
  logsExecucao <<-  add_row(logsExecucao, id=as.numeric(patient.info[1]),uuid=patient.info[2],patientid=patient.info[3],full_name=patient.info[5],accao=action)
  # logsExecucao <<- rbind.fill(logsExecucao, temp)  gera registos multipos no log
  
}

#' Compoe um vector com dados do paciente que se vai actualizar
#' 
#' @param df tabela de duplicados para extrair os dados do Pat
#' @param index row do paciente em causa 
#' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
#' @examples composePatientToUpdate(67, nids_dups)
composePatientToUpdate <- function(index,df){
  
  id = df$id[index]
  uuid = df$uuid[index]
  patientid = df$patientid[index]
  openmrs_patient_id =df$patient_id[index]
  full.name =  df$full_name[index]
  patient <- c(id,uuid,patientid,openmrs_patient_id,full.name,index)
  patient
}

#' Compoe um vector com dados do paciente para se utilizar na tabela dos logs
#' 
#' @param df tabela onde se vai extrair os dados 
#' pode ser tabela do openmrs/idart/ ou uma composta
#' @param index row do paciente em causa 
#' @return vector[id,uuid,patientid,openmrs_patient_id,full.name,index] 
#' @examples composePatientToLog(67, nids_dups)
composePatientToLog <- function(df,index){
  
  id = df$id[index]
  uuid = df$uuid[index]
  patientid = df$patientid[index]
  full_name <- str_replace_na(paste0(df$firstnames[index], ' ',df$lastname[index]),replacement=' ')
  patient <- c(id,uuid,patientid,full_name)
  patient
}

#' Actualiza os  uuid e openmrsuuid na tabela Patient
#' 
#' @param patient.id id do paciente na tabela  Patient
#' @param uuid.openmrs uuid do  OpenMRS
#' @return 0/1  (0) - error  (1) - sucess   
#' @examples updateUUID(67,  uuid)
updateUUID <- function (postgres.con, patient.id, uuid) {
  dbExecute(
    postgres.con,
    paste0(
      "update  public.patient set uuid='",
      uuid,
      "' , openmrsuuid='",
      uuid ,
      "'  where id = ",
      patient.id,
      " ;"
    )
  )
  
}


#' Actualiza os  dados do uuidopenmrs    na tabela Patient
#' 
#' @param patient.id id do paciente na tabela  Patient
#' @param uuid.openmrs uuid do  OpenMRS
#' @return 0/1  (0) - error  (1) - sucess   
#' @examples updateUuuispenMRS(67,  uuid)
updateUuidOpenMRS <- function (patient.id, uuid.openmrs) {
  dbExecute(
    con_postgres,
    paste0(
      "update  public.patient set uuidopenmrs='",
      uuid.openmrs,
      "'  where id = ",
      patient.id,
      " ;"
    )
  )
  
}


#' Inicia o processo de actualizacao de um nid na base de dados iDART e OpenMRS 
#' 
#' @param new.nid.act NID do paciente por actualizar
#' @param patient_to_update  informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]
#' @param df.idart.patients datafrane  com informacao de todos pacientes no iDART (info de identificacao e estado do paciente)
#' @param con.openmrs objecto de conexao com a bd mysql
#' @param con.idart objecto de  conexao com bd postgresql
#' @return Unknown
#' @examples beginUpdateProcess(new.nid.act,patient_to_update,df.idart.patients , con.openmrs,con.idart)
beginUpdateProcess <- function(new.nid.act,patient_to_update,df.idart.patients , df.openmrs.patients,con.openmrs,con.idart){
  
  if(new.nid.act != 0) {
    
    if( checkIfExistsNidIdart(new.nid.act,df.idart.patients) ){
      
      new.nid.act <- getNewNid(new.nid.act)
      status_act_idart <- actualizaNidiDART(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new.nid.act )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
        
      }
      else{
        
        logAction(patient_to_update, paste0('PostgreSQL -  Nao foi Actualizar o NID no iDART:',patient_to_update[3]))
        
      }
      
      
    }
    else if (checkIfExistsNidOpenMRS(new.nid.act,df.openmrs.patients) ){
    
      new.nid.act <- getNewNid(new.nid.act)
      status_act_idart <- actualizaNidiDART(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new.nid.act )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
        
      }
      else{
        
        logAction(patient_to_update, paste0('PostgreSQL - Nao foi Actualizar o NID no iDART:',patient_to_update[3]))
        
      }
      
    }
    else {
      
      status_act_idart <- actualizaNidiDART(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = new.nid.act )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
        
      }
      else{
        
        logAction(patient_to_update, paste0('PostgreSQL -  Nao foi Actualizar o NID no iDART:',patient_to_update[3]))
        
      }
    }
  } 
  else{
    
    logAction(patient_to_update, 'Nao foi possivel obter novo NID para  o paciente. Verificar o Formato do NID!')
  }
  
}



#' Busca o nome da US do openmrs
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
getOpenmrsDefaultLocation <- function (openmrs.con){
  resut_set <- dbSendQuery(openmrs.con, "select property_value from global_property where property= 'default_location'")
  data <- fetch(resut_set,n=1)
  openmrs_default_location <- data$property_value[1]
  RMySQL::dbClearResult(resut_set)
  rm(data,resut_set)
  #detach("package:RMySQL", unload = TRUE)
  return (openmrs_default_location)
  
}

#' Verifica se os ficheiros necessarios para executar as operacoes existem
#' 
#' @param files  nomes dos ficheiros
#'  @param dir  directorio onde ficam os files
#' @return TRUE/FALSE
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
checkScriptsExists <- function (files, dir){
for(i in 1:length(files)){
  f <- files[i]
  if(!file.exists(paste0(dir,f))){
    message(paste0('Erro - Ficheiro ', f, ' nao existe em ',dir))
    return(FALSE)
  }
}
  return(TRUE)
}

