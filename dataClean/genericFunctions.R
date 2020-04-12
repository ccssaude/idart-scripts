require(plyr)

#' Escreve  os logs das accoes executadas nas DBs iDART/OpenMRS numa tabela logsExecucao
#' 
#' @param patient.info informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]   
#' @param action descricao das accoes executadas sobre o paciente
#' @return append uma row na tabela logs
#' @examples
#' logAction(patientToUpdate, ' Paciente com NID X Alrerado Para NID Y')
logAction <- function (patient.info,action){
  
  
  temp =   add_row(logsExecucao, id=as.numeric(patient.info[1]),uuid=patient.info[2],patientid=patient.info[3],full_name=patient.info[5],accao=action)
  logsExecucao <<- rbind.fill(logsExecucao, temp)
  
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


#' Inicia o processo de actualizacao de um nid na base de dados iDART e OpenMRS 
#' 
#' @param new.nid NID do paciente por actualizar
#' @param patient_to_update  informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]
#' @param df.idart.patients datafrane  com informacao de todos pacientes no iDART (info de identificacao e estado do paciente)
#' @param con.openmrs objecto de conexao com a bd mysql
#' @param con.idart objecto de  conexao com bd postgresql
#' @return Unknown
#' @examples beginUpdateProcess(new.nid.act,patient_to_update,df.idart.patients , con.openmrs,con.idart)
beginUpdateProcess <- function(new.nid.act,patient_to_update,df.idart.patients , con.openmrs,con.idart){
  
  if(new.nid.act != 0) {
    
    if( checkIfExistsNidIdart(new.nid.act,df.idart.patients) ){
      
      new.nid.act <- getNewNid(new.nid.act)
      status_act_idart <- actualizaNidiDART(con.idart = con.openmrs,patient.to.update =patient_to_update,new.nid = new.nid.act )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
        
      }
      else{
        
        logAction(patient_to_update, paste0('Nao foi Actualizar o NID no iDART:',patient_to_update[3]))
        
      }
      
      
    }
    else {
      
      status_act_idart <- actualizaNidiDART(con.idart = con.openmrs,patient.to.update =patient_to_update,new.nid = new.nid.act )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new.nid.act)
        
      }
      else{
        
        logAction(patient_to_update, paste0('Nao foi Actualizar o NID no iDART:',patient_to_update[3]))
        
      }
    }
  } 
  else{
    
    logAction(patient_to_update, 'Nao foi possivel obter novo NID para  o paciente. Verificar o Formato do NID!')
  }
  
}
