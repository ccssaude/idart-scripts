

source('paramConfiguration.R')           ## Carregar as configuracoes



## Pacientes
## Buscar todos pacientes OpenMRS & iDART
openmrsAllPatients <- getAllPatientsOpenMRS(con_openmrs)
openmrsAllPatients$identifierSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   
idartAllPatients <- getAllPatientsIdart(con_postgres)
idartAllPatients$patientidSemLetras <- sapply(idartAllPatients$patientid, removeLettersFromNid)    
openmrsAllPatients$patientidSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   

# Busca o codigo da US
us.code= getOpenmrsUsCode(openmrsAllPatients)                 


# NIDs Mal formatados -> nids com tamanho diferente de 21
nidsPorFormatar <- openmrsAllPatients[which(nchar(openmrsAllPatients$patientidSemLetras)<21),]  
nidsPorFormatar$nid_formatado <- ""
nidsPorFormatar <- nidsPorFormatar[,c(1,2,3,4,5,6,7,9,14,15)]





if(tipo_nid=='seq/ano') {
 
  nidsPorFormatar$nid_formatado <-  sapply(nidsPorFormatar$patientidSemLetras , formatNidMisau )
} else if (tipo_nid == 'ano/seq')
{
  
  nidsPorFormatar$nid_formatado <-  sapply(nidsPorFormatar$patientidSemLetras , formatNidMisauV1 )
} else {
  message(
    " Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)"
  )
  
}


# Guardar um excell com nids por corrigir manualemnte
if (dim(nidsPorFormatar)[1]>0){
  
  corrigir_manualmente_nid<- nidsPorFormatar[which(nidsPorFormatar$nid_formatado==0),]
  us.name <- getOpenmrsDefaultLocation(con_openmrs) 
  
  write_xlsx(
    corrigir_manualmente_nid,
    path = paste0(us.name,' - Pacientes para corrigir Manualmente os nids.xlsx'),
    col_names = TRUE,
    format_headers = TRUE
  )
  nidsPorFormatar <- subset(nidsPorFormatar,!(nidsPorFormatar$uuid %in% corrigir_manualmente_nid$uuid), )
  rm(corrigir_manualmente_nid)
}




# Verificar se o nid Formatado nao existe na BD , para nao duplicar
nidsPorFormatar$nid_formatado_status <- ""
nidsPorFormatar$nid_formatado_status <- sapply(nidsPorFormatar$nid_formatado,checkNidExistsOpenmrsIdart)
#names(nidsPorFormatar)[which(names(nidsPorFormatar)=='full_name_openmrs')] <- 'full_name'

# Cruzar os uuids  do Openmrs com iDART
nids_formatar_idart_openmrs=inner_join(x=nidsPorFormatar,y=idartAllPatients, by=c('uuid'))
                                       
### Formata os nids que nao criam conflitos 
nao_existe_openmrs_idart <- subset(nids_formatar_idart_openmrs,nid_formatado_status=='nao_existe' & nchar(nid_formatado)==21,)


if(dim(nao_existe_openmrs_idart)[1]>0 ){
  
  for (i in 1:dim(nao_existe_openmrs_idart)[1]) {
    
    nid_formatado <- nao_existe_openmrs_idart$nid_formatado[i]
    
    patient_to_upate <-  composePatientToUpdate(index = i,df = nao_existe_openmrs_idart)
    
    exec <- actualizaNidiDART(con.idart = con_postgres,patient.to.update = patient_to_upate,new.nid = nid_formatado)
    
    if(exec==1){
      
      actualizaNidOpenMRS(con.openmrs = con_openmrs,patient.to.update = patient_to_upate,new.nid = nid_formatado)
      
      
    }
    
  }
  
}



##############################################################################################################################################################
##############################################################################################################################################################
##  Exportar os Logs
if (dim(logsExecucao)[1]>0){
  

  write_xlsx(
    logsExecucao,
    path = paste0(us.name, ' - log_formatacao_nids_openmrs.xlsx'),
    col_names = TRUE,
    format_headers = TRUE
  )
  
  
  

