
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

#' Busca dados do paciente com a data de  Lev menos recentes na tabela/dataframe dos duplcados
#' 
#' @param index index dos pacientes duplicados
#' @param nid NID dos pacientes duplicados 
#' @return vector com [id,uuid,patientid,openmrs_patient_id,full.name,index]  
#' @examples getPacLevMenosRecente([12,34],'0110201001/2007/07819')
getPacLevMenosRecente <- function(index, nid) {
  data_levanta_menos_rec <-
    min(dups_idart_openmrs[index, ]$data_ult_levant) # data de lev menos recente entre os 2 pac duplcds
  
  if (is.na(data_levanta_menos_rec) |  dups_idart_openmrs[index[1], ]$data_ult_levant == dups_idart_openmrs[index[2], ]$data_ult_levant  ) {  

      index_pat_min_act = index[1]   # Nenhum dos pacientes tem data de levantamento, actualiza  o primeiro da lista | os 2 pac tem a mesma data do ultimo levantamento
      patient <-
        c(
          dups_idart_openmrs$id[index_pat_min_act],
          dups_idart_openmrs$uuid[index_pat_min_act],
          dups_idart_openmrs$patientid[index_pat_min_act],
          dups_idart_openmrs$patient_id[index_pat_min_act],
          dups_idart_openmrs$full_name[index_pat_min_act],
          index_pat_min_act,
          dups_idart_openmrs$firstnames[index_pat_min_act],
          dups_idart_openmrs$lastname[index_pat_min_act]
        )
      return(patient)
    } 
     else {
    
    index_pat_min_act <-
      which(
        dups_idart_openmrs$data_ult_levant == data_levanta_menos_rec &
          dups_idart_openmrs$patientid == nid
      )
    patient <-
      c(
        dups_idart_openmrs$id[index_pat_min_act],
        dups_idart_openmrs$uuid[index_pat_min_act],
        dups_idart_openmrs$patientid[index_pat_min_act],
        dups_idart_openmrs$patient_id[index_pat_min_act],
        dups_idart_openmrs$full_name[index_pat_min_act],
        index_pat_min_act,
        dups_idart_openmrs$firstnames[index_pat_min_act],
        dups_idart_openmrs$lastname[index_pat_min_act]
      )
    return(patient)
  }
  
}

#' Busca dados do paciente com  a data do ultimo Lev mais recentes na tabela/dataframe dos duplicados
#' 
#' @param index index dos pacientes duplicados
#' @param nid NID dos pacientes duplicados
#' @return vector com [id,uuid,patientid,openmrs_patient_id,full.name,index]  
#' @examples getPacLevMaisRecente([12,34],'0110201001/2007/07819')
getPacLevMaisRecente <- function(index, nid) {
  data_levanta_rec <-
    max(dups_idart_openmrs[index, ]$data_ult_levant) # data de lev menos recente entre os 2 pac duplcds
  
  if (is.na(data_levanta_rec) | dups_idart_openmrs[index[1], ]$data_ult_levant == dups_idart_openmrs[index[2], ]$data_ult_levant) {

      ## os 2 pacientes nao tem a data do ult_levant | os 2 pac tem as datas do ultimo levantamento iguais
      index_pat_mais_rec = index[1]   ## Consider o primeiro
      patient <-
        c(
          dups_idart_openmrs$id[index_pat_mais_rec],
          dups_idart_openmrs$uuid[index_pat_mais_rec],
          dups_idart_openmrs$patientid[index_pat_mais_rec],
          dups_idart_openmrs$patient_id[index_pat_mais_rec],
          dups_idart_openmrs$full_name[index_pat_mais_rec],
          index_pat_mais_rec,
          dups_idart_openmrs$firstnames[index_pat_mais_rec],
          dups_idart_openmrs$lastname[index_pat_mais_rec]
        )
      return(patient)
    } else {
      
    index_pat_mais_rec <-
      which((dups_idart_openmrs$data_ult_levant) == data_levanta_rec &
              dups_idart_openmrs$patientid == nid
      )
    patient <-
      c(
        dups_idart_openmrs$id[index_pat_mais_rec],
        dups_idart_openmrs$uuid[index_pat_mais_rec],
        dups_idart_openmrs$patientid[index_pat_mais_rec],
        dups_idart_openmrs$patient_id[index_pat_mais_rec],
        dups_idart_openmrs$full_name[index_pat_mais_rec],
        index_pat_mais_rec,
        dups_idart_openmrs$firstnames[index_pat_mais_rec],
        dups_idart_openmrs$lastname[index_pat_mais_rec]
      )
    return(patient)
  }
}

#' Busca o paciente nao activo entre os duplicados
#' 
#' @param index index dos pacientes duplicados
#' @param nid NID dos pacientes duplicados 
#' @return vector com [id,uuid,patientid,openmrs_patient_id,full.name,index]  
#' @examples getPacInactivo([12,34],'0110201001/2007/07819')
getPacActivo<- function(index, nid) {
  index_pat_ctivo <- index[ which((dups_idart_openmrs[index, ]$estado_tarv %in% c('ACTIVO NO PROGRAMA','TRANSFERIDO DE')) & 
                               dups_idart_openmrs[index, ]$patientid == nid ) ] #  index do paciente  activo
 
      patient <-
        c(
          dups_idart_openmrs$id[index_pat_ctivo],
          dups_idart_openmrs$uuid[index_pat_ctivo],
          dups_idart_openmrs$patientid[index_pat_ctivo],
          dups_idart_openmrs$patient_id[index_pat_ctivo],
          dups_idart_openmrs$full_name[index_pat_ctivo],
          index_pat_ctivo,
          dups_idart_openmrs$firstnames[index_pat_ctivo],
          dups_idart_openmrs$lastname[index_pat_ctivo]
        )
      return(patient)
    }
    
#' Busca o paciente Inactivo entre os duplicados
#' 
#' @param index index dos pacientes duplicados
#' @param nid NID dos pacientes duplicados 
#' @return vector com [id,uuid,patientid,openmrs_patient_id,full.name,index]  
#' @examples getPacInactivo([12,34],'0110201001/2007/07819')
getPacInactivo <- function(index, nid) {
  
  index_pat_inactivo <- index[ which( ( dups_idart_openmrs[index, ]$estado_tarv %in% c('TRANSFERIDO PARA','SUSPENSO','ABANDONO','OBITO','OUTRO') | is.na(dups_idart_openmrs[index, ]$estado_tarv) )  & 
                                       dups_idart_openmrs[index, ]$patientid == nid ) ]    #  index do paciente Nao activo
  

  patient <-
    c(
      dups_idart_openmrs$id[index_pat_inactivo],
      dups_idart_openmrs$uuid[index_pat_inactivo],
      dups_idart_openmrs$patientid[index_pat_inactivo],
      dups_idart_openmrs$patient_id[index_pat_inactivo],
      dups_idart_openmrs$full_name[index_pat_inactivo],
      index_pat_inactivo,
      dups_idart_openmrs$firstnames[index_pat_inactivo],
      dups_idart_openmrs$lastname[index_pat_inactivo]
    )
  return(patient)
}



##  TODO
##  Melhorar este code
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
          patient <- composePatientToLog(index = v,dups_mesmo_uuid_nid_diff)
          solucao <- paste0("G2.7-CC  Nid do paciente e nomes mudaram para NID: ",dups_mesmo_uuid_nid_diff$identifier[v], ", Nome: ",dups_mesmo_uuid_nid_diff$given_name[v],
                        " , Apelido: ",dups_mesmo_uuid_nid_diff$family_name[v])
          logAction(patient.info = patient , action = solucao )
          
          df.duplicados <-subset(df.duplicados, !(df.duplicados$id %in% dups_mesmo_uuid_nid_diff$id[v]),) 
          
        }
    }
    
    
    return(df.duplicados) 
     
    }
    
    else {
    
      df.duplicados # same as return(df)
    
  }
}

#' Guarda a sugestao de troca nid, na coluna solucao do df dos duplicados
#' 
#' @param index index dos pacientes duplicados
#' @param solucao - a melhor solucao encontrada baseado no estado de permanencia dos dois pacientes 
#' @return NA  
#' @examples logSolucao([12,34],'Trocar o nid do paciente x, por ser duplicado e ter o estado tarv: abandono')
logSolucao <- function (index_1,index_2, solucao){
  
  
  dups_idart_openmrs$solucao[index_1] <<- solucao
  dups_idart_openmrs$solucao[index_2] <<- solucao
  
}
