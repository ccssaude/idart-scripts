library(tibble)
library(writexl)
# Verifica e actualiza pacientes que tem dados no iDART diferentes do OpenMRS
# --  cidalia joao
# ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

wd <- '~/R/iDART/idart-scripts/dataClean/'
 
# Limpar o envinronment
rm(list=setdiff(ls(), "wd"))

if (dir.exists(wd)){
  
  setwd(wd)  
  source('paramConfiguration.R')     
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}

########################         # Actualizar  os dados dos pacientes da FARMAC                                  ########################
#########################################################################################################################################
# TRUE/FALSE se a us referencia pacientes para farmac dve actualizar os dados nas tabelas da farmac 
#  primeiro copiar o nome da clinicname e actualizar openmrs para US que referenciam para farmac 
referencia.farmac <- TRUE    # modificar para TRUE/FALSE

#por_actualizar <- subset(por_actualizar, ! patientid %in% farmac_patiens$patientid , )
# se faz referencia a farmac deve actualizar os dados dos pacientes da farmac tambem
if(referencia.farmac){
  
  # Apagar todas prescricoes que nao sao da us local
  dbExecute(con_postgres, paste0("delete from sync_temp_dispense where sync_temp_dispenseid <> '", us.name, "' ;") )
  
  source('correcao_nids_farmac.R')
  
  ## Remover pacientes da farmac para nao criar confusao
  farmac_patients <- dbGetQuery(con_postgres,paste0("select distinct patientid, patientfirstname , patientlastname
                                                  from (select distinct patientid, patientfirstname , patientlastname from
                                                  sync_temp_dispense sd  where sync_temp_dispenseid='", us.name, "' union all 
                                                  select distinct patientid, firstnames as  patientfirstname , lastname as patientlastname from
                                                  sync_temp_patients sp ) all_p order by all_p.patientid"))
  
  farmac_patients <- add_column(farmac_patients, observacao="")
  farmac_patients <- add_column(farmac_patients, new_nid="")

  
}

########################         # Actualizar  os Pacientes com UUID diferentes iDART/OpenMRS mas com NIDs iguais  ######################
#########################################################################################################################################
# garantir que ocampo uuidopenmrs =uuid sejam iguais

dbExecute(con_local,'update patient set uuidopenmrs =uuid')

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
    # actualiza os dados nas tabelas da farmac tambem
    
    # se faz referencia a farmac deve actualizar os dados dos pacientes da farmac tambem
    if(referencia.farmac){
      
      if(patient_to_update[3] %in% farmac_patients$patientid){
        
        dbExecute(con_postgres, paste0("update  sync_temp_patients set patientid = '",new_nid,
                                       "' , firstnames = '",
                                       patient_to_update[7], 
                                       "' , lastname = '",
                                       patient_to_update[8],
                                       "' , uuid ='",
                                       patient_to_update[2],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        dbExecute(con_postgres, paste0("update  sync_temp_dispense set patientid = '",new_nid,
                                       "' , patientfirstname = '",
                                       patient_to_update[7], 
                                       "' , patientlastname = '",
                                       patient_to_update[8],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        
        
        
        index_sync_tem_dispense <-which( no_farmac_patients$patientid==patient_to_update[3])
        index_sync_tem_patient <-which( no_match_patients$patientid==patient_to_update[3])
        
        if(length(index_sync_tem_dispense)>0){
          no_farmac_patients$novo_nid_2[index_sync_tem_dispense[1]] <- new_nid
        }
        if(length(index_sync_tem_patient)>0){
          no_match_patients$novo_nid_2[index_sync_tem_patient[1]] <- new_nid
        }
        # farmac_patients$observacao[index[1]]<- paste0("Dados actualizados em sync_tem_patients & sync_temp_dispense NID: ",new_nid)
        #  farmac_patients$new_nid[index[1]]<- new_nid
      }
      
    }
    
    
  }
}

rm(info_incosistente)
rm(existe_openmrs)

if(dim(logsExecucao)[1]> 0){
  
  logs_tmp_1 <- logsExecucao

  
}
########################   # Actualizar dados de todos pacientes do iDART com mesmo  uuid.openmrs =uuid.idart ,   ########################
                           # mas NIDS sao diferentes                                                                ########################
#########################################################################################################################################

idartAllPatients <- getAllPatientsIdart(con_postgres)
por_actualizar <- inner_join(idartAllPatients,temp,by='uuid')
por_actualizar <- por_actualizar[which(por_actualizar$patientid != por_actualizar$identifier),]

#### comeca a processar 
if(dim(por_actualizar)[1]>0){
  

  for (v in 1:dim(por_actualizar)[1]) {
    new_nid <- gsub(x =  por_actualizar$identifier[v],pattern = "'",replacement = '')
    
    patient_to_update= composePatientToUpdateNomeNid(index = v,df = por_actualizar)
    updatePatientIdart(con.idart = con_postgres,patient.to.update = patient_to_update,new.nid =new_nid )
    
    # actualiza os dados nas tabelas da farmac tambem
    
    # se faz referencia a farmac deve actualizar os dados dos pacientes da farmac tambem
    if(referencia.farmac){
      
      if(patient_to_update[3] %in% farmac_patients$patientid){
        
        dbExecute(con_postgres, paste0("update  sync_temp_patients set patientid = '",new_nid,
                                       "' , firstnames = '",
                                       patient_to_update[7], 
                                       "' , lastname = '",
                                       patient_to_update[8],
                                       "' , uuid ='",
                                       patient_to_update[2],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        dbExecute(con_postgres, paste0("update  sync_temp_dispense set patientid = '",new_nid,
                                       "' , patientfirstname = '",
                                       patient_to_update[7], 
                                       "' , patientlastname = '",
                                       patient_to_update[8],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        
        dbExecute(con_postgres, paste0("update  packagedruginfotmp set patientid = '",new_nid,
                                       "' , patientfirstname = '",
                                       patient_to_update[7], 
                                       "' , patientlastname = '",
                                       patient_to_update[8],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        
        index_sync_tem_dispense <-which( no_farmac_patients$patientid==patient_to_update[3])
        index_sync_tem_patient <-which( no_match_patients$patientid==patient_to_update[3])
        
        if(length(index_sync_tem_dispense)>0){
          no_farmac_patients$novo_nid_2[index_sync_tem_dispense[1]] <- new_nid
        }
        if(length(index_sync_tem_patient)>0){
          no_match_patients$novo_nid_2[index_sync_tem_patient[1]] <- new_nid
        }
       # farmac_patients$observacao[index[1]]<- paste0("Dados actualizados em sync_tem_patients & sync_temp_dispense NID: ",new_nid)
      #  farmac_patients$new_nid[index[1]]<- new_nid
      }
      
    }

  }
  
}

rm (por_actualizar)
if(dim(logsExecucao)[1] > 0){
  
  logs_tmp_2 <- logsExecucao #  guardar os logs num dataframe separado a cada actualizacao da BD
  
}

########################               # Pacientes que estao no iDART sem uuid no openmrs                        ########################
#########################################################################################################################################
# Sempre que se fizer uma actualizacao no iDART & no OpenMRS deve-se carregar os dados atualizados

idartAllPatients <- getAllPatientsIdart(con_postgres)

#carrefa a tabela de logs vazia
load(file = 'logs/logsExecucao.Rdata')
por_investigar <- idartAllPatients[ which(!idartAllPatients$uuid %in% temp$uuid),]

# investigar pelo nid
existe_por_investigar <- inner_join(por_investigar,temp,by=c('patientid'='identifier'))

# remover pacientes que existem se pesquisarmos pelo nid
por_investigar <- por_investigar[which(!por_investigar$patientid %in% existe_por_investigar$patientid ),]
rm(existe_por_investigar)

# remover paciente em transito

if(dim(por_investigar)[1] > 0){
  
  por_investigar <-  por_investigar[which(  !por_investigar$startreason %in%  c('Paciente em Transito',' Inicio na maternidade')),]
  dfTemp <-  por_investigar[which(grepl(pattern = "TR", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_2 <- por_investigar[which( grepl( pattern = "VIS",ignore.case = TRUE,x = por_investigar$patientid ) == TRUE  ),]
  dfTemp_3 <- por_investigar[which(grepl(  pattern = "VIA", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  dfTemp_4<-  por_investigar[which(grepl(pattern = "T", ignore.case = TRUE,x = por_investigar$patientid ) == TRUE ),]
  
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_2$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_3$patientid),]
  por_investigar <-  por_investigar[which(!por_investigar$patientid %in% dfTemp_4$patientid),]
  rm(dfTemp_2, dfTemp,dfTemp_3,dfTemp_4)
  

  # Remover pacientes sem dispensa, provavelmente sejam transitos/abandonos/obitos/transferidos/para   ########
  por_investigar$totaldispensas[which(is.na(por_investigar$totaldispensas))] <- 0 
  por_investigar <- por_investigar[which(por_investigar$totaldispensas>2 ),]
  
  
}


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

# Remover numeros dos nomes
temp$full_name_openmrs <- sapply(temp$full_name_openmrs , removeNumbersFromName )
temp$full_name_openmrs <- sub(pattern = '  ',replacement = " ", x = temp$full_name_openmrs)

for (i in 1:dim(por_investigar)[1]) {
  nome <- por_investigar$full_name[i]
  
  temp$string_dist <-   mapply(getStringDistance, temp$full_name_openmrs,  nome)
  
  index = which(as.numeric(temp$string_dist)  == as.numeric(min(temp$string_dist))  )
  
    if (length(index) > 1) {
    index <- index[1]
    }
  
  por_investigar$uuid[i]   <-          temp$uuid[index]
  por_investigar$patient_id[i]   <-    temp$patient_id[index]
  por_investigar$identifier[i]   <-    temp$identifier[index]
  por_investigar$given_name[i]   <-    temp$given_name[index]
  por_investigar$middle_name[i]  <-    temp$middle_name[index]
  por_investigar$family_name[i]  <-    temp$family_name[index]
  por_investigar$openmrs_full_name[i]  <- temp$full_name_openmrs[index]
  por_investigar$estado_tarv[i]  <-    temp$estado_tarv[index]
  por_investigar$string_dist[i] <-    temp$string_dist[index]
  por_investigar$openmrs_uuid[i] <-    temp$uuid[index]
  
}

########################    Os pacientes que tem string_dis < 0.08 (margem de erro de 1%) sao iguais devemos primeiro verificar se o paciente se o NID do
########################     openmrs ja existe no iDART, se existir a solucao e unir os pacientes ########
#########################################################################################################################################

matched <- subset(por_investigar,as.numeric(string_dist) < 0.1, )

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
    #holahola
    
    # se faz referencia a farmac deve actualizar os dados dos pacientes da farmac tambem
    if(referencia.farmac){
      
      if(patient_to_update[3] %in% farmac_patients$patientid){
        
        dbExecute(con_postgres, paste0("update  sync_temp_patients set patientid = '",new_nid,
                                       "' , firstnames = '",
                                       patient_to_update[7], 
                                       "' , lastname = '",
                                       patient_to_update[8],
                                       "' , uuid ='",
                                       patient_to_update[2],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        dbExecute(con_postgres, paste0("update  sync_temp_dispense set patientid = '",new_nid,
                                       "' , patientfirstname = '",
                                       patient_to_update[7], 
                                       "' , patientlastname = '",
                                       patient_to_update[8],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
        
        
        dbExecute(con_postgres, paste0("update  packagedruginfotmp set patientid = '",new_nid,
                                       "' , patientfirstname = '",
                                       patient_to_update[7], 
                                       "' , patientlastname = '",
                                       patient_to_update[8],
                                       "'  where patientid = '",
                                       patient_to_update[3], "' ;" )  )
       # index <-which( farmac_patients$patientid==patient_to_update[3])
       # farmac_patients$observacao[index[1]]<- paste0("Dados actualizados em sync_tem_patients & sync_temp_dispense NID: ",new_nid)
       # farmac_patients$new_nid[index[1]]<- new_nid
        
      }
      
    }
  }
  
  }
  por_investigar <- por_investigar[which(! por_investigar$patientid %in% matched$patientid) , ]
} else{ rm(matched)}

########################    Os pacientes que tem string_dis > 0.08 (margem de erro de 1%) verificamos o padrao do nid e compraramos
#########################   esse padrao com os NIDS do Openmrs ex; 793/12 se o nid no OpenMRS ofr 011105701/2012/00793 entao trata-se do mesmo paciente
########################     openmrs ja existe no iDART, se existir a solucao e unir os pacientes ########
#########################################################################################################################################

### pacientes que tem 7 ou caracteres no nid aqueles com 5 ou 4 excluimos porque fica dificil comparar
sequencia_comparavel  <- por_investigar[which(nchar(por_investigar$patientid)>=7),]

if(dim(sequencia_comparavel)[1] > 0){
  
  sequencia_comparavel$num_seq <- ""
  
  if(tipo_nid == 'Seq/Ano') {
    sequencia_comparavel$num_seq  <-
      sapply(sequencia_comparavel$patientid , getNumSeqNid)
    
  } else if (tipo_nid == 'Ano/Seq') {
    sequencia_comparavel$num_seq  <-
      sapply(sequencia_comparavel$patientid , getNumSeqNidV1)

  }   else {
    message(
      " Nao foi possivel obter o tipo de Sequencia automaticamente
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano')
            geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )"
    )
    stop(
      "Nao foi possivel obter o tipo de Sequencia automaticamente
       verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes
         casos  costuma ser tipo_nid = 'Ano/Seq' )"
    )
    
  }
  
  vec_nids_matched <- c()
  
  sequencia_comparavel$identifier <- sapply(sequencia_comparavel$identifier, removeLettersFromNid)
  
  for (i in 1:dim(sequencia_comparavel)[1]) {
  
    seq <- sequencia_comparavel$num_seq[i]
    
    if(seq != 0 &&   grepl(pattern = substr(seq, 2,nchar(seq)),x = sequencia_comparavel$identifier[i],ignore.case = TRUE )    && ( as.numeric(sequencia_comparavel$string_dist[i]) <= 0.15) ) { # trara-se do mesmo pacientes
      
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
        
        
      } 
      else{
        
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

  
} else { rm(por_investigar) }



}


########################    guardas os logs
#########################################################################################################################################
##  Exportar os Logs
if (dim(logsExecucao)[1]>0){
  
  pacintes_erro_ss<-  logsExecucao[which(grepl(pattern = 'SS-CM',x=logsExecucao$accao,ignore.case = TRUE)),] 
  pacintes_dups_apenas_idart_unir_com_outro <-  logsExecucao[which(grepl(pattern = '3.1.2:-CM',x=logsExecucao$accao,ignore.case = TRUE)),] 
  pacintes_erro_sql <-  logsExecucao[which(grepl(pattern = 'BD_ERROR',x=logsExecucao$accao,ignore.case = TRUE)),] 
  
  
  if(dim(pacintes_dups_apenas_idart_unir_com_outro)[1]>0){
    
    write_xlsx(
      pacintes_dups_apenas_idart_unir_com_outro,
      path = paste0('output/',us.name,' - Pacientes_duplicados_apenas_idart_unir_com_outro.xlsx'),
      col_names = TRUE,
      format_headers = TRUE
    )
    logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_dups_apenas_idart_unir_com_outro$uuid)),]
  }
  if(dim(pacintes_erro_ss)[1]>0){
    
    write_xlsx(
      pacintes_erro_ss,
      path = paste0('output/',us.name,' - Pacientes_idart_que_nao_existem_openmrs.xlsx'),
      col_names = TRUE,
      format_headers = TRUE
    )
    logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_erro_ss$uuid)),]
  }
  if(dim(pacintes_erro_sql)[1]>0){
    
    write_xlsx(
      pacintes_erro_sql,
      path = paste0('output/' , 'Formatacao_NIDs_',us.name,' - Pacientes_que_tiveram_erro_sql_durante_a_correcao_fazer_roll_back.xlsx'),
      col_names = TRUE,
      format_headers = TRUE
    )
    logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_erro_sql$uuid)),]
  } 
  if(referencia.farmac){
    
    if(nrow(no_match_patients)>0){
      write_xlsx(x =no_match_patients,path = 'output/sync_temp_dispense_actualizados.xlsx' )
    }
    if(nrow(no_farmac_patients)>0){
      write_xlsx(x =no_farmac_patients,path = 'output/sync_temp_patient_actualizados.xlsx' )
    }
   
  }
  
if(exists("logs_tmp_1") && dim(logs_tmp_1)[1] > 0){
  if(exists("logs_tmp_2") && dim(logs_tmp_2)[1] > 0){
    tmp <- rbind.fill(logs_tmp_1,logs_tmp_2)
    write_xlsx(tmp, path = paste0('output/',us.name,'_log_formatacao_nids.xlsx',  col_names = TRUE,
                                  format_headers = TRUE)  )
    save( tmp ,file =paste0('output/',us.name,'_log_formatacao_nids.RData') )
    rm(logs_tmp_1,logs_tmp_2)
  } else{
    write_xlsx(logs_tmp_1, path = paste0('output/',us.name,'iDART_log_actualizacao_nids_do_openmrs.xlsx',  col_names = TRUE,
                                         format_headers = TRUE )  )
    save(logs_tmp_1 ,file =paste0('output/',us.name,'iDART_log_actualizacao_nids_do_openmrs.RData' ))
    rm(logs_tmp_1)
    
  }
} else {
  
  if(exists("logs_tmp_2") && dim(logs_tmp_2)[1] > 0){
    write_xlsx(logs_tmp_2, path = paste0('output/',us.name,'iDART_log_actualizacao_nids_do_openmrs.xlsx',  col_names = TRUE,
                                         format_headers = TRUE)  )
    save(logs_tmp_2 ,file =paste0('output/',us.name,'iDART_log_actualizacao_nids_do_openmrs.RData') )
  rm(logs_tmp_2)
  }
  
}
  
  save(list = ls(),file =gsub(pattern = ' ', replacement = '_',x = paste0('output/Environment_',us.name, '_.RData') ))
  setwd(paste0(wd,'output'))
  # >Zip all files
  zip(zipfile = gsub(pattern = ' ', replacement = '_',x = paste0('zip_',us.name)), files = dir() )
  
  
}
