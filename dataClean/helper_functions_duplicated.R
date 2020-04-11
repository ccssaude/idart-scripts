
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


##  Duplicados no iDART e OpenMRS
getDuplicatesPatOpenMRS <- function(connection)  {
  rs  <-    dbSendQuery(
    connection,
    paste0(
      "
SELECT  
pe.uuid uuid,
pid.identifier Nid, 
pn.given_name,
pn.middle_name,
pn.family_name,
  concat(lower(pn.given_name),' ' ,if(lower(pn.middle_name) is not null,concat( lower(pn.middle_name), ' ') ,' '),lower(pn.family_name)) as full_name_openmrs ,
pd.birthdate as data_nasc,
	ROUND(DATEDIFF(NOW(),pd.birthdate)/365) idade_actual,
      DATE_FORMAT(pa.data_inicio,'%d/%m/%Y') inicio_tarv,
      IF(inscrito_pretarv.date_enrolled IS NULL,'NAO','SIM') inscrito_pre_tarv,
            IF(inscrito_tarv.date_enrolled IS NULL,'NAO','SIM') inscrito_tarv,
             estado.estado AS estado_tarv,
             estado.start_date AS data_estado
  FROM  patient pat INNER JOIN patient_identifier pid ON pat.patient_id =pid.patient_id
  INNER JOIN person pe ON pat.patient_id=pe.person_id
  INNER JOIN person_name pn ON pe.person_id=pn.person_id and pn.voided=0

  LEFT JOIN
	(
		SELECT person_id,gender,birthdate FROM person
		WHERE voided=0
		GROUP BY person_id

	) pd ON pd.person_id = pn.person_id
LEFT JOIN
(SELECT 	pg.patient_id,date_enrolled data_inicio
					FROM 	patient p INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
					WHERE 	pg.voided=0 AND p.voided=0 AND program_id=2
					) pa ON pa.patient_id= pat.patient_id
LEFT JOIN
			(
				SELECT 	pgg.patient_id,pgg.date_enrolled
				FROM 	patient pt INNER JOIN patient_program pgg ON pt.patient_id=pgg.patient_id
				WHERE 	pgg.voided=0 AND pt.voided=0 AND pgg.program_id=1
			) inscrito_pretarv  ON inscrito_pretarv.patient_id=pat.patient_id
LEFT JOIN
			(
				SELECT 	pgg.patient_id,pgg.date_enrolled
				FROM 	patient pt INNER JOIN patient_program pgg ON pt.patient_id=pgg.patient_id
				WHERE 	pgg.voided=0 AND pt.voided=0 AND pgg.program_id=2
			) inscrito_tarv  ON inscrito_tarv.patient_id=pat.patient_id

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

WHERE pid.identifier
IN( SELECT  pid.identifier
  FROM patient_identifier pid
  WHERE pid.voided=0
  GROUP BY identifier
HAVING COUNT(*)>=2 ) and pid.preferred=1 AND pat.voided=0  ORDER BY   pid.identifier "
    )
  )
  
  data <- fetch(rs, n = -1)
  RMySQL::dbClearResult(rs)
  return(data)
}

getDuplicatesPatiDART <- function(connection)  {
  dups  <-    dbGetQuery(
    connection,
    paste0(
      "
  select
  pat.id, 
  pat.uuid,
  pat.uuidopenmrs,
  pat.patientid,dateofbirth::TIMESTAMP::DATE as dateofbirth,
  lower(pat.firstnames) as firstnames , 
  lower(pat.lastname) as lastname , 
  CONCAT (lower(pat.firstnames),' ',  lower(pat.lastname)) as full_name,
  pat.sex,
  ep.startreason,
  dispensas.total as total_levantamentos
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
       ) dispensas on dispensas.patientid = pat.patientid
WHERE pat.patientid
IN( SELECT  pat.patientid
  FROM patient pat
  GROUP BY pat.patientid
HAVING COUNT(*)>=2 ) order by pat.patientid

 "
    )
  )
  
  dups
}


getPacLevMenosRecente <- function(index,nid){
  
  data_levanta_menos_rec <-  min(dups_idart_openmrs[index,]$data_ult_levant) # data de lev menos recente entre os 2 pac duplcds
  
  if(is.na(data_levanta_menos_rec)){ 
    index_pat_min_act <- which(is.na(dups_idart_openmrs$data_ult_levant) & dups_idart_openmrs$patientid==nid)
   c(dups_idart_openmrs$id[index_pat_min_act],dups_idart_openmrs$uuid[index_pat_min_act],dups_idart_openmrs$full_name[index_pat_min_act])
   
  } else {
    index_pat_min_act <- which(dups_idart_openmrs$data_ult_levant==data_levanta_menos_rec & dups_idart_openmrs$patientid==nid)
    c(dups_idart_openmrs$id[index_pat_min_act],dups_idart_openmrs$uuid[index_pat_min_act],dups_idart_openmrs$full_name[index_pat_min_act])
   }
 
}

getPacLevMaisRecente <- function(index,nid){
  
  data_levanta_rec <-  max(dups_idart_openmrs[index,]$data_ult_levant) # data de lev menos recente entre os 2 pac duplcds
  
  if(is.na(data_levanta_rec)){ 
    index_pat_mais_rec <- which(!is.na(dups_idart_openmrs$data_ult_levant) & dups_idart_openmrs$patientid==nid)
    c(dups_idart_openmrs$id[index_pat_mais_rec],dups_idart_openmrs$uuid[index_pat_mais_rec],dups_idart_openmrs$full_name[index_pat_mais_rec])
  } else {
    
    index_pat_mais_rec <- which((dups_idart_openmrs$data_ult_levant)==data_levanta_rec & dups_idart_openmrs$patientid==nid)
    c(dups_idart_openmrs$id[index_pat_mais_rec],dups_idart_openmrs$uuid[index_pat_mais_rec],dups_idart_openmrs$full_name[index_pat_mais_rec])
    
  }
}

updatePatSameUuuidDifNid <- function(df.duplicados,con.postgres){
  
  dups_mesmo_uuid_nid_diff <- df.duplicados[which(df.duplicados$patientid!=df.duplicados$identifier)]
  
  
  
  if( dim(dups_mesmo_uuid_nid_diff)[1]>0 ){
    
    
    for (v in 1:dim(dups_mesmo_uuid_nid_diff)[1]) {
      
      out <- tryCatch(
        {
          
          
          # Just to highlight: if you want to use more than one 
          # R expression in the "try" part then you'll have to 
          # use curly brackets.
          # 'tryCatch()' will return the last evaluated expression 
          # in case the "try" part was completed successfully
            nid_novo <- dups_mesmo_uuid_nid_diff$identifier[v]
            nome_novo <- paste0(dups_mesmo_uuid_nid_diff$given_name[v]," ",dups_mesmo_uuid_nid_diff$middle_name[v] )
            apelido_novo <-dups_mesmo_uuid_nid_diff$family_name[v]
            id  <- dups_mesmo_uuid_nid_diff$id[v]
            message(paste0("Actualizando dados do paciente: ",dups_mesmo_uuid_nid_diff$patientid[v], " para NID:", nid_novo,",  Nome: ",nome_novo, " ",apelido_novo))
            dbExecute(con.postgres,paste0("update  public.patientidentifier set value ='",nid_novo,  "' where patient_id = ",  id, ";"   ))
            dbExecute(con.postgres,paste0("update  public.patient set patientid = '", nid_novo,  "' where id = ",  id, ";"   ))
            dbExecute(con.postgres,paste0("update  public.patient set firstnames = '", nome_novo,  "' , lastname ='", apelido_novo, "' where id = ",  id, ";"   ))
         
            
          },
          error=function(cond) {
            
            message("Nao foi possivel Actualizar o NID do paciente":dups_mesmo_uuid_nid_diff$patientid[v] )
            message(cond)
            # Choose a return value in case of error
            return(NA)
          },
          warning=function(cond) {
            message("Here's the original warning message:")
            message(cond)
            # Choose a return value in case of warning
            return(NULL)
          },
          finally={
            # NOTE:
            # Here goes everything that should be executed at the end,
            # regardless of success or error.
            # If you want more than one expression to be executed, then you 
            # need to wrap them in curly brackets ({...}); otherwise you could
            # just have written 'finally=<expression>' 
            # message(paste("Processed URL:", url))
            # message("Some other message at the end")
          }
        )
        
        if(out==1){
          ## Remover pacientes corrigidos da tabela dos pacientes duplicados
          ## Usei a a clausula not in ! %in%
          logAction(df = df.duplicados,index =which(dups_idart_openmrs$id==dups_mesmo_uuid_nid_diff$id[v]),
                    action = paste0("G2.7-CC  Nid do paciente e nomes mudaram para NID: ",dups_mesmo_uuid_nid_diff$identifier[v], ", Nome: ",dups_mesmo_uuid_nid_diff$given_name[v],
                                    " , Apelido: ",dups_mesmo_uuid_nid_diff$family_name[v]) )
          df.duplicados <-subset(df.duplicados, !(df.duplicados$id %in% dups_mesmo_uuid_nid_diff$id[v]),) 
          
        }
    }
    
    
    return(df.duplicados) 
     
    }
    
    else {
    
      df.duplicados # same as return(df)
    
  }
}


SetSolucao <- function (index_1,index_2, solucao){
  
  
  dups_idart_openmrs$solucao[index_1] <- solucao
  dups_idart_openmrs$solucao[index_2] <- solucao
  
}


#' Escreve  os logs das accoes executadas nas DBs iDART/OpenMRS numa tabela logsExecucao
#' 
#' @param df tabela de duplicados para extrair alguns dados 
#' @param index row do paciente em causa na tabela dos duplicados.
#' @param action descricao das accoes executadas sobre o paciente
#' @return append uma row na tabela logs
#' @examples
#' actualizaNidiDART(con_idart, '0111030701/2014/00065',56,'0111030701/2016/00087')
logAction <- function (df,index,action){
  
 id = df$id[index]
 uuid = df$uuid[index]
 patient.id = df$patientid[index]
 full.name =  df$full_name[index]
 temp =   add_row(logsExecucao, id=id,uuid=uuid,patientid=patient.id,full_name=full.name,accao=action)
 logsExecucao <<- rbind.fill(logsExecucao, temp)
 
}

