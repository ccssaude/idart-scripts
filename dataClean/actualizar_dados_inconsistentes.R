

########################         # Actualizar primeiro os Pacientes com UUID diferentes iDART/OpenMRS            ########################
#########################################################################################################################################
temp <- getPatientsInvestigar(con_openmrs)

info_incosistente  <- idartAllPatients[which(!idartAllPatients$uuid %in% temp$uuid),]

existe_openmrs <- inner_join(info_incosistente,temp,by=c('patientid'='identifier'))
existe_openmrs <- existe_openmrs[ , -which(names(existe_openmrs) %in% c("uuid.x"))]
names(existe_openmrs)[names(existe_openmrs) == "uuid.y"] <- "uuid"

# Actualizar pacietens do iDART sem uuid , Com NID existente no OpenMRS

if(dim(existe_openmrs)[1] > 0){
  for (i in 1:dim(existe_openmrs)[1]) {
    nid <- gsub(x =  existe_openmrs$patientid[i] ,pattern = "'",replacement = '')
    patient_to_update <- composePatientToUpdateNomeNid(i,df=existe_openmrs)
    updatePatientIdart(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid = nid )
    
  }
}

rm(info_incosistente)
rm(existe_openmrs)

if(dim(logsExecucao)[1]> 0){
  
  logs_tmp_1 <- logsExecucao

  
}

########################   # Actualizar dados de todos pacientes do iDART com mesmo  uuid.openmrs =uuid.idart, buscar dados do OpenMRS ##
#########################################################################################################################################

source('paramConfiguration.R')

por_actualizar <- inner_join(idartAllPatients,temp,by='uuid')

por_actualizar <- por_actualizar[which(por_actualizar$patientid != por_actualizar$identifier),]


if(dim(por_actualizar)[1]>0){
  
  for (v in 1:dim(por_actualizar)[1]) {
    new_nid <- gsub(x =  por_actualizar$identifier[v],pattern = "'",replacement = '')
    patient_to_update= composePatientToUpdateNomeNid(index = v,df = por_actualizar)
    updatePatientIdart(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid =new_nid )
    
  }
  
}

rm (por_actualizar)

if(dim(logsExecucao)[1] > 0){
  
  logs_tmp_2 <- logsExecucao
  
}



########################               # Pacientes que estao no iDART sem uuid no openmrs                        ########################
#########################################################################################################################################
idartAllPatients <- getAllPatientsIdart(con_postgres)

por_investigar <- idartAllPatients[ which(!idartAllPatients$uuid %in% temp$uuid),]

existe_por_investigar <- inner_join(por_investigar,temp,by=c('patientid'='identifier'))
por_investigar <- por_investigar[which(!por_investigar$patientid %in% existe_por_investigar$patientid ),]
rm(existe_por_investigar)

########################                remover paciente sem transito                                            ########################
#########################################################################################################################################
por_investigar <-  por_investigar[which(  !por_investigar$startreason %in%  c('Paciente em Transito',' Inicio na maternidade')),]
dfTemp <-  por_investigar[which(grepl(pattern = "TR", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
dfTemp_2 <- por_investigar[which( grepl( pattern = "VIS",ignore.case = TRUE,x = por_investigar$patientid ) == TRUE  ),]
dfTemp_3 <- por_investigar[which(grepl(  pattern = "VIA", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp$patientid),]
por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_2$patientid),]
por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_3$patientid),]
rm(dfTemp_2, dfTemp,dfTemp_3)



########################    # Remover pacientes sem dispensa, provavelmente sejam transitos/abandonos/obitos/transferidos/para   ########
#########################################################################################################################################
por_investigar$totaldispensas[which(is.na(por_investigar$totaldispensas))] <- 0 
por_investigar <- por_investigar[which(por_investigar$totaldispensas>3 ),]


########################    Aplicar algoritmo de similaridade de texto nos nomes dos pacientes iDART/ OpenMRS de modo a encontrar
########################    a contraparte OpenMRS ########
#########################################################################################################################################
if(dim(por_investigar)[1] > 0) {
  
por_investigar$full_name <- gsub( pattern = 'NA',replacement = '',x = paste0(por_investigar$firstnames, ' ', por_investigar$lastname) )
por_investigar$full_name <-  sapply(por_investigar$full_name  ,  removeNumbersFromName)
por_investigar$string_dist <- ""
por_investigar$openmrs_uuid <- ""
por_investigar$patient_id <- ""
por_investigar$identifier <- ""
por_investigar$given_name <- ""
por_investigar$middle_name <- ""
por_investigar$family_name <- ""
por_investigar$openmrs_full_name <- ""
por_investigar$estado_tarv <- ""

temp$full_name_openmrs <- sapply(temp$full_name_openmrs , removeNumbersFromName )


for (i in 1:dim(por_investigar)[1]) {
  nome <- por_investigar$full_name[i]
  temp$string_dist <-   mapply(getStringDistance, temp$full_name_openmrs,  nome)
  
  index = which(as.numeric(temp$string_dist)  == as.numeric(min(temp$string_dist))  )
  
    if (length(index) == 2) {
    index <- index[1]
    }
  
  por_investigar$uuid[i]   <-    temp$uuid[index]
  por_investigar$patient_id[i]   <-    temp$patient_id[index]
  por_investigar$identifier[i]   <-    temp$identifier[index]
  por_investigar$given_name[i]   <-    temp$given_name[index]
  por_investigar$middle_name[i]  <-    temp$middle_name[index]
  por_investigar$family_name[i]  <-    temp$family_name[index]
  por_investigar$openmrs_full_name[i]  <- temp$full_name_openmrs[index]
  por_investigar$estado_tarv[i]  <-    temp$estado_tarv[index]
  por_investigar$string_dist[i] <-    temp$string_dist[index]
}

########################    Os pacientes que tem string_dis < 0.08 (margem de erro de 1%) sao iguais devemos primeiro verificar se o paciente se o NID do
########################     openmrs ja existe no iDART, se existir a solucao e unir os pacientes ########
#########################################################################################################################################

matched <- subset(por_investigar,as.numeric(string_dist) < 0.08, )

if(dim(matched)[1]>0){
  
  for (v in 1:dim(matched)[1]) {
    
  old_nid <- matched$patientid[v]
  old_name <-  matched$full_name[v]
  new_nid <- gsub(x =  matched$identifier[v],pattern = "'",replacement = '')
  new_name <-  matched$openmrs_full_name[v] 
  patient_to_update <- composePatientToUpdateNomeNid(v,matched) 
  
  if(new_nid %in% idartAllPatients$patientid){
   
     solucao <- paste0("3.1.2:-CM  Unir no iDART o paciente: (", new_nid ,' - ',new_name, ') ',
                       'com: (', old_nid,' - ' , old_name ,")", " preferido e (", new_nid ,' - ',new_name, ')' )
    updatePackagedrugInfoTmp(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid = new_nid)
    logAction(patient.info = patient_to_update,action = solucao)
  
   
  } 
  else{
   
    updatePatientIdart(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid = new_nid) 
    
  }
  
  }
  por_investigar <- por_investigar[which(! por_investigar$patientid %in% matched$patientid) , ]
}
else{ rm(matched)}

########################    Os pacientes que tem string_dis > 0.08 (margem de erro de 1%) verificamos o padrao do nid e compraramos
#########################   esse padrao com os NIDS do Openmrs ex; 793/12 se o nid no OpenMRS ofr 011105701/2012/00793 entao trata-se do mesmo paciente
########################     openmrs ja existe no iDART, se existir a solucao e unir os pacientes ########
#########################################################################################################################################

### pacientes que tem 7 ou caracteres no nid aqueles com 5 ou 4 excluimos porque fica dificil comparar
sequencia_comparavel  <- por_investigar[which(nchar(por_investigar$patientid)>=7),]

if(dim(sequencia_comparavel)[1] > 0){
  
  vec_nids_matched <- c()
  sequencia_comparavel$num_seq <- ""
  sequencia_comparavel$identifier <- sapply(sequencia_comparavel$identifier, removeLettersFromNid)
  for (i in 1:dim(sequencia_comparavel)[1]) {
    
    if(tipo_nid=='Seq/Ano'){
      
      seq <-  getNumSeqNid(sequencia_comparavel$patientid[i])
      
    }  else if(tipo_nid=='Ano/Seq'){
      
      seq <-  getNumSeqNidV1(sequencia_comparavel$patientid[i])
      
    } else {
      message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
      break
    }
    
    if(seq != 0 && (seq %in% sequencia_comparavel$identifier[i])  && ( as.numeric(sequencia_comparavel$string_dist[i]) <= 0.15) ) { # trara-se do mesmo pacientes
      
      old_nid <- sequencia_comparavel$patientid[i]
      old_name <-  sequencia_comparavel$full_name[i]
      new_nid <- gsub(x =  sequencia_comparavel$identifier[i],pattern = "'",replacement = '')
      new_name <-  sequencia_comparavel$openmrs_full_name[i] 
      patient_to_update <- composePatientToUpdateNomeNid(i,sequencia_comparavel)
      
      if(new_nid %in% idartAllPatients$patientid){
        
        solucao <- paste0("3.1.2:-CM  Unir no iDART o paciente: (", new_nid ,' - ',new_name, ') ',
                          'com: (', old_nid,' - ' , old_name ,")", " preferido e (", new_nid ,' - ',new_name, ')' )
        updatePackagedrugInfoTmp(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid = new_nid)
        logAction(patient.info = patient_to_update,action = solucao)
        
        
      } else{
        
        updatePatientIdart(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid = new_nid) 
        
      }
      vec_nids_matched <<- c(vec_nids_matched, old_nid )
    }
    
    
  }
  
  
  if(length(vec_nids_matched)>0){
    por_investigar <- por_investigar[which(! por_investigar$patientid %in% vec_nids_matched ),]
  }

  
}
else{ rm(sequencia_comparavel) }

if(dim(por_investigar)[1]>0){
  for (var in 1:dim(por_investigar)[1]) {
    total_lev <- por_investigar$totaldispensas[var]
    estado_tarv <- por_investigar$estado_tarv[var]
    patient_to_update <- composePatientToUpdateNomeNid(var, por_investigar)
    
    if(total_lev < 10 && estado_tarv %in% c("ABANDONO"," TRANSFERIDO PARA", "OBITO" ) ){
      # Descarta estes pacientes
      
    } else {
      
      solucao <- paste0("SS-CM - Paciente: ",patient_to_update[3], ' - ', patient_to_update[5], " Esta no iDart Mas nao existe no OpenMRS, Investigar e corrigir informacao manualmente")
      logAction(patient.info = patient_to_update,action = solucao)
    }
    
  }

  
}
else { rm(por_investigar) }

} else{ rm(por_investigar) }


########################    guardas os logs
#########################################################################################################################################

if(exists("logs_tmp_1") && dim(logs_tmp_1)[1] > 0){
  if(exists("logs_tmp_2") && dim(logs_tmp_2)[1] > 0){
    save(rbind.fill(logs_tmp_1,logs_tmp_2) ,file =paste0(us.name,'Actualizacoes_dados_pacientes.RData') )
    rm(logs_tmp_1,logs_tmp_2)
  } else{
    save(logs_tmp_1 ,file =paste0(us.name,'Actualizacoes_dados_pacientes.RData') )
    rm(logs_tmp_1)
    
  }
} else if(exists("logs_tmp_2") && dim(logs_tmp_2)[1] > 0){
  save(logs_tmp_2 ,file =paste0(us.name,'Actualizacoes_dados_pacientes.RData') )
  rm(logs_tmp_2)
  
  
}