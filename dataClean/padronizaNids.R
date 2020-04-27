

source('paramConfiguration.R')                     ##  1 -Carregar as configuracoes

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



# excluir nids que formatando criam duplicados openmrs
index_gera_udps_openmrs <- which(nidsPorFormatar$nid_formatado %in% duplicadosOpenmrs$Nid)

gera_dups_openmrs <-  nidsPorFormatar[index_gera_udps_openmrs,]

nidsPorFormatar <-subset(nidsPorFormatar, !(nidsPorFormatar$nid_formatado %in% duplicadosOpenmrs$Nid),)  

# excluir nids que formatando criam duplicados idart
index_gera_udps_idart <- which(nidsPorFormatar$nid_formatado %in% duplicadosiDART$patientid)
gera_dups_idart <-  nidsPorFormatar[index_gera_udps_idart,]
nidsPorFormatar <-subset(nidsPorFormatar, !(nidsPorFormatar$nid_formatado %in% duplicadosiDART$patientid),)  

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
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
          
        }

      }
      else if (checkIfExistsNidOpenMRS(new_nid,openmrsAllPatients) ){
        
        new_nid <- getNewNid(new_nid)
        status_act_idart <- updatePatientIdart(con.idart = con.idart,patient.to.update =patient_to_update,new.nid = new_nid )
        
        if(status_act_idart==1){
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
          
        }
        
      }
      else {
        
        status_act_idart <- updatePatientIdart(con.idart = con_postgres,patient.to.update =patient_to_update,new.nid = new_nid )
        
        if(status_act_idart==1){
          actualizaNidOpenMRS(con.openmrs = con.openmrs,patient.to.update = patient_to_update,new.nid = new_nid)
          
        }
      }
    } 
    else{
      
      logAction(patient_to_update, paste0('ERROR_NID - Error - Nao foi possivel obter novo NID para  o paciente:',nidsPorFormatar$identifier[i] ,', Verifique o Formato do NID! & corrige Manualmente'))
    }
    
}



# Guardar um excell com nids por corrigir manualemnte

##############################################################################################################################################################
##############################################################################################################################################################
##  Exportar os Logs

  
  formatar_manualmente <- logsExecucao[which(grepl(pattern = 'ERROR_NID',x=logsExecucao$accao,ignore.case = TRUE)),]
  nids_formatados <-  nidsPorFormatar[which(nidsPorFormatar$uuid %in% logsExecucao$uuid),c('patient_id','uuid','identifier','full_name_openmrs','nid_formatado')]
  if(dim(nids_formatados)[1]>0){
    
  write_xlsx(
    nids_formatados,
    path = paste0(us.name, ' - Nids Formatados no Padrao do MISAU.xlsx'),
    col_names = TRUE,
    format_headers = TRUE
  ) }
  
  if(dim(formatar_manualmente)[1]>0){
  
  
  
    write_xlsx(
      formatar_manualmente,
      path = paste0(us.name, ' - Pacientes com nids para formatar manualmente.xlsx'),
      col_names = TRUE,
      format_headers = TRUE
    ) }
  

