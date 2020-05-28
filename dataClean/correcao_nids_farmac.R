library(dplyr)
library(writexl)
library(tibble)

sync_temp_dispense <- dbGetQuery(con_postgres, "select patientid, patientfirstname, patientlastname from  sync_temp_dispense 
                                 where sync_temp_dispenseid = 'CS ALBASINE'   
                                 group by patientid ,patientfirstname, patientlastname ;")

farmac_patients <- getAllPatientsFarmac(con_postgres)

# pacientes que estao na tabela sync_tem_patients e nao estao no tbl patiens
no_farmac_patients  <- anti_join(farmac_patients ,idartAllPatients , by='patientid')
no_farmac_patients <- add_column( no_farmac_patients, observacao = '')
no_farmac_patients <- add_column( no_farmac_patients, novo_nid ='')
no_farmac_patients <- add_column( no_farmac_patients, novo_nid_2 ='')
# pacientes que estao na sync_temp_dispense mas nao estao na table a patiens
no_match_patients <- anti_join(sync_temp_dispense,idartAllPatients , by='patientid')
no_match_patients <- add_column( no_match_patients, observacao ='')
no_match_patients <- add_column( no_match_patients, novo_nid ='')
no_match_patients <- add_column( no_match_patients, novo_nid_2 ='')
# pacientes da sync_tem_dispense que  nao estao na table a sync_temp_patients mas estao na tabela patients
no_match_patients_in_tbb_pat <- inner_join(idartAllPatients, no_match_patients,  by='patientid', keep =TRUE) 

#match por nome e actualizar patientid na tabela sync_tem_dispense
if(nrow(no_match_patients)>0){
  
  for (i in 1:nrow(no_match_patients)) {
    
    patient_id <- no_match_patients$patientid[i]
    f_name<- no_match_patients$patientfirstname[i]
    l_name <-  no_match_patients$patientlastname[i]
    #uuid <-  no_match_patients$uuid[i]
    # match por nome e apelid
    match <- idartAllPatients[which(tolower(idartAllPatients$firstnames)==tolower(f_name) & tolower(idartAllPatients$lastname)==tolower(l_name)),]
    
    if(nrow(match)==1){

      new_patient_id = match$patientid[1]
      
      dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
      no_match_patients$observacao[i] <- paste0('actualizado para: ',new_patient_id)
      message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      no_match_patients$novo_nid[i] <- new_patient_id
    }
    else if(nrow(match)==2){
      
      if(match$uuid[1]==match$uuid[2]){
        new_patient_id = match$patientid[1]
        dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        no_match_patients$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
        no_match_patients$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        
        no_match_patients$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e duplicado."
      }
    
      
    }
    else if(nrow(match)==0){
      
      no_match_patients$observacao[i] <- "este paciente nao existe na tabela patient. analizar e resolver manualmente"
      
    }
    else if(nrow(match)==3){
      
      if(match$patientid[1]==match$patientid[2] & match$patientid[2]==match$patientid[3] ){
        new_patient_id = match$patientid[1]
        dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        no_match_patients$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
        no_match_patients$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        
        no_match_patients$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
      }
      
      
    }
    else {
      

      no_match_patients$observacao[i] <- "e o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
    }
    
    
  }
  write_xlsx(x = no_match_patients,path = 'output/correcao_sync_temp_patients.xlsx')
  
}


#match por nome e actualizar patientid na tabela sync_tem_patient
if(nrow(no_farmac_patients)>0){
  
  for (i in 1:nrow(no_farmac_patients)) {
    
    patient_id <- no_farmac_patients$patientid[i]
    f_name<- no_farmac_patients$firstnames[i]
    l_name <-  no_farmac_patients$lastname[i]
    #uuid <-  no_match_patients$uuid[i]
    # match por nome e apelid
    match <- idartAllPatients[which(tolower(idartAllPatients$firstnames)==tolower(f_name) & tolower(idartAllPatients$lastname)==tolower(l_name)),]
    
    if(nrow(match)==1){
      
      new_patient_id = match$patientid[1]
      
      dbExecute(con_postgres, paste0("update sync_temp_patients set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
      no_farmac_patients$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
      message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      no_farmac_patients$novo_nid[i] <- new_patient_id
    }
    else if(nrow(match)==2){
      
      if(match$uuid[1]==match$uuid[2]){
        new_patient_id = match$patientid[1]
        dbExecute(con_postgres, paste0("update sync_temp_patients set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        no_farmac_patients$observacao[i] <-  paste0('actualizado para: ',new_patient_id)
        no_farmac_patients$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        
        no_farmac_patients$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e duplicado."
      }
      
      
    }
    else if(nrow(match)==0){
      
      no_farmac_patients$observacao[i] <- "este paciente nao existe na tabela patient. analizar e resolver manualmente"
      
    }
    else if(nrow(match)==3){
      
      if(match$patientid[1]==match$patientid[2] & match$patientid[2]==match$patientid[3] ){
        new_patient_id = match$patientid[1]
        dbExecute(con_postgres, paste0("update sync_temp_patients set patientid='",new_patient_id,"' where patientid = '",patient_id, "' ;") )
        no_farmac_patients$observacao[i] <- paste0('actualizado para: ',new_patient_id)
        no_farmac_patients$novo_nid[i] <- new_patient_id
        message(paste0("Farmac actualizando dados do paciente: ", patient_id,' para : ',new_patient_id))
      } else {
        
        no_farmac_patients$observacao[i] <- " o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
      }
      
      
    }
    else {
      
      
      no_farmac_patients$observacao[i] <- "e o nid deste pacieente nao existe na tabela patient, e o match dos nomes e triplicado"
    }
    
    
  }
  
  write_xlsx(x = no_farmac_patients,path = 'output/correcao_sync_temp_dispense.xlsx')
  
  
}

