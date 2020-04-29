# Versao modificada somente para polanacanico
# verifica-se com frequencia nids sem barra mas que a sequencia obdece o formarto do MISA ex: 011103172019399, 011103172019608
## Formata NIDs incorrectos OpenMRS & IDART
## Caso nao consiga formatar escreve no Log e sugere correcao manual (openmrs & IDART)


wd <- '~/R/iDART/idart-scripts/dataClean/'

# Limpar o envinronment

rm(list=setdiff(ls(), c("wd", "tipo_nid") ))

if (dir.exists(wd)){
  
  setwd(wd) 
  source('paramConfiguration.R')                     ##  Carrega dados actualizados
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}




## Pacientes Duplicados Por NID iDART & OpenMRS
duplicadosOpenmrs <- getDuplicatesPatOpenMRS(con_openmrs)
duplicadosiDART <-   getDuplicatesPatiDART(con_postgres)


# NIDs Mal formatados -> nids com tamanho diferente de 21
nidsPorFormatar <- openmrsAllPatients[which(nchar(openmrsAllPatients$patientidSemLetras)<21),]  
nidsPorFormatar$nid_formatado <- ""
nidsPorFormatar <- nidsPorFormatar[,c(1,2,3,4,5,6,7,9,14,15)]

if(dim(nidsPorFormatar)[1] > 0){}

if(tipo_nid=='Seq/Ano') {
  
  nidsPorFormatar$nid_formatado <-  sapply(nidsPorFormatar$patientidSemLetras , formatNidMisau )
} else if (tipo_nid == 'Ano/Seq')
{
  
  nidsPorFormatar$nid_formatado <-  sapply(nidsPorFormatar$patientidSemLetras , formatNidMisauV1 )
} else {
  message(
    " Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)"
  )
  stop(' Nao especificou o tipo de NID  (ano/seq   ou seq/ano) ')
  
}


# excluir nids que estao na df dos duplicados  openmrs
index_gera_udps_openmrs <- which(nidsPorFormatar$identifier %in% duplicadosOpenmrs$Nid)
gera_dups_openmrs <-  nidsPorFormatar[index_gera_udps_openmrs,]
nidsPorFormatar <-subset(nidsPorFormatar, ! nidsPorFormatar$identifier %in%  duplicadosOpenmrs$Nid,)

# excluir nids que estao na df dos duplicados idart
index_gera_udps_idart <- which(nidsPorFormatar$identifier %in% duplicadosiDART$patientid )
gera_dups_idart <-  nidsPorFormatar[index_gera_udps_idart,]
nidsPorFormatar <-subset(nidsPorFormatar, ! nidsPorFormatar$identifier %in% duplicadosiDART$patientid ,)




nidsPorFormatar <- inner_join(nidsPorFormatar,idartAllPatients,by='uuid', keep=TRUE)


#vector[id,uuid,patientid,openmrs_patient_id,full.name]
#  beginUpdateProcess(new.nid.act,patient_to_update,df.idart.patients , con.openmrs,con.idart)

for (i in 1:dim(nidsPorFormatar)[1]) {
  
  new_nid <- nidsPorFormatar$nid_formatado[i]
  patient_to_update <- composePatientToUpdateNomeNid(i,nidsPorFormatar)
  
  if(new_nid != 0) {
    
    if( checkIfExistsNidIdart(new_nid,idartAllPatients) ){
      
      new_nid <- getNewNid(new_nid)
      status_act_idart <- updatePatientIdart(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = new_nid )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs = con_openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
        
      }
      
    }
    else if (checkIfExistsNidOpenMRS(new_nid,openmrsAllPatients) ){
      
      new_nid <- getNewNid(new_nid)
      status_act_idart <- updatePatientIdart(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new_nid )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs =  con_openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
        
      }
      
    }
    else {
      
      status_act_idart <- updatePatientIdart(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = new_nid )
      
      if(status_act_idart==1){
        actualizaNidOpenMRS(con.openmrs =  con_openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
        
      }
    }
  } 
  else{
    padrao_nids_sem_barra = '01110317'
    
    if(grepl(pattern = padrao_nids_sem_barra,x = nidsPorFormatar$identifier[i],ignore.case = TRUE ) &&
       grepl(pattern = '2019',x = nidsPorFormatar$identifier[i],ignore.case = TRUE && 
             str_count(nidsPorFormatar$identifier[i], '/') ==0   ) ) {
      
      index_year <- stri_locate_first(str = nidsPorFormatar$identifier[i], regex = '2019')[1]
      us_code <- substr(nidsPorFormatar$identifier[i], start = 0, stop = index_year -1 )
      us_code <- formatUsCodeV1(us_code)
      
      if(us_code !=0 ){
        
        year <- '2019'
        index_seq <- stri_locate_first(str = nidsPorFormatar$identifier[i], regex = '2019')[2]
        seq <-  substr(nidsPorFormatar$identifier[i], start = index_seq + 1 , stop = nchar(nidsPorFormatar$identifier[i]) )
        seq <- formatSequencia(seq)
        
        if(seq !=0 ){
          
          nid_formatado <-  paste0(us_code, "/",year , "/", seq )
          
          
          if(checkNidFormatMisau(nid_formatado) &&  !checkIfExistsNidIdart(nid_formatado,idartAllPatients) && !checkIfExistsNidOpenMRS(nid_formatado,openmrsAllPatients)  ){
            
            nidsPorFormatar$nid_formatado[i] <- nid_formatado
              
            status_act_idart <- updatePatientIdart(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = nid_formatado )
            
            if(status_act_idart==1){
              actualizaNidOpenMRS(con.openmrs =  con_openmrs,patient.to.update = patient_to_update,new.nid = nid_formatado)
              
            }
            
          }  else{
            
            logAction(patient_to_update, paste0('ERROR_NID - Error - Nao foi possivel obter novo NID para  o paciente:',nidsPorFormatar$identifier[i] ,', Verifique o Formato do NID! & corrige Manualmente no iDART e OpenMRS'))
            
          }
          
          
        }  else{
          
          logAction(patient_to_update, paste0('ERROR_NID - Error - Nao foi possivel obter novo NID para  o paciente:',nidsPorFormatar$identifier[i] ,', Verifique o Formato do NID! & corrige Manualmente no iDART e OpenMRS'))
          
        }
       
        
      }  else{
        
        logAction(patient_to_update, paste0('ERROR_NID - Error - Nao foi possivel obter novo NID para  o paciente:',nidsPorFormatar$identifier[i] ,', Verifique o Formato do NID! & corrige Manualmente no iDART e OpenMRS'))
        
      }

      
    } else{
      
      logAction(patient_to_update, paste0('ERROR_NID - Error - Nao foi possivel obter novo NID para  o paciente:',nidsPorFormatar$identifier[i] ,', Verifique o Formato do NID! & corrige Manualmente no iDART e OpenMRS'))
      
    }
    

  }
  
}


# Guardar um excell com nids por corrigir manualemnte
##############################################################################################################################################################
##############################################################################################################################################################
##  Exportar os Logs


formatar_manualmente <- logsExecucao[which(grepl(pattern = 'ERROR_NID',x=logsExecucao$accao,ignore.case = TRUE)),]
nids_formatados <-  nidsPorFormatar[which(nidsPorFormatar$uuid %in% logsExecucao$uuid && (nidsPorFormatar$nid_formatado !=0)  ),c('patient_id','uuid','identifier','full_name_openmrs','nid_formatado')]
if(dim(nids_formatados)[1]>0){
  
  write_xlsx(
    nids_formatados,
    path = paste0('output/',us.name, ' - Nids Formatados no Padrao do MISAU.xlsx'),
    col_names = TRUE,
    format_headers = TRUE
  ) }

if(dim(formatar_manualmente)[1]>0){
  
    write_xlsx(
    formatar_manualmente,
    path = paste0('output/', us.name, ' - Pacientes com nids para formatar manualmente.xlsx'),
    col_names = TRUE,
    format_headers = TRUE
  ) }



