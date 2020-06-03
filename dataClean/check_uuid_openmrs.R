# pacientes com mesmo nid mas uuid e diferente idartuuid != personuuid

####################### diferenca de uuids entre pacientes   (uuid do idart e diferente uuid openmrs  mas os nids sao iguais) ###############################
#############################################################################################################################################################

different_uuid <- inner_join(idartAllPatients, temp, by=c('patientid'='identifier')) %>% select( id, patientid, dateofbirth,firstnames,
                                                                                                 sex, lastname, startreason,totaldispensas, uuid.x, uuid.y,given_name,birthdate,
                                                                                                 middle_name, family_name, full_name_openmrs, estado_tarv,ult_levant_idart,
                                                                                                 data_ult_levant, data_prox_marcado ) 
# formatar a data do ult_levant_idart
different_uuid$ult_levant_idart <- substr(as.character(different_uuid$ult_levant_idart ),1,10)

# mudart o nome das colunas uuid
names(different_uuid)[names(different_uuid) == "uuid.y"] <- "uuidopenmrs"
names(different_uuid)[names(different_uuid) == "uuid.x"] <- "uuididart"

# remover transitos
if(dim(different_uuid)[1] > 0){
  
  different_uuid <-  different_uuid[which(  !different_uuid$startreason %in%  c('Paciente em Transito',' Inicio na maternidade')),]
  dfTemp <-  different_uuid[which(grepl(pattern = "TR", ignore.case = TRUE,x = different_uuid$patientid ) == TRUE ),]
  dfTemp_2 <- different_uuid[which( grepl( pattern = "VIS",ignore.case = TRUE,x = different_uuid$patientid ) == TRUE  ),]
  dfTemp_3 <- different_uuid[which(grepl(  pattern = "VIA", ignore.case = TRUE,x = different_uuid$patientid ) == TRUE ),]
  dfTemp_4<-  different_uuid[which(grepl(pattern = "T", ignore.case = TRUE,x = different_uuid$patientid ) == TRUE ),]
  
  different_uuid <-  different_uuid[which(!different_uuid$patientid %in% dfTemp$patientid),]
  different_uuid <-  different_uuid[which(!different_uuid$patientid %in% dfTemp_2$patientid),]
  different_uuid <-  different_uuid[which(!different_uuid$patientid %in% dfTemp_3$patientid),]
  different_uuid <-  different_uuid[which(!different_uuid$patientid %in% dfTemp_4$patientid),]
  rm(dfTemp_2, dfTemp,dfTemp_3,dfTemp_4)
  
  
  # Remover pacientes sem dispensa, provavelmente sejam transitos/abandonos/obitos/transferidos/para   ########
  different_uuid$totaldispensas[which(is.na(different_uuid$totaldispensas))] <- 0 
  
  different_uuid <- different_uuid[which(different_uuid$totaldispensas>2 ),]
  
  # remover pacientes abandonos
  
}


#different_uuid <- different_uuid[different_uuid$uuidopenmrs != "",]
#different_uuid <- different_uuid[different_uuid$uuididart != different_uuid$uuidopenmrs, ]


different_uuid <- different_uuid %>% select(id, patientid,firstnames,lastname, uuididart,uuidopenmrs, full_name_openmrs)

if(nrow(different_uuid)>0){
  different_uuid <- different_uuid[!is.na(different_uuid$patientid),]
  different_uuid$duplicado_openmrs <- ''
  different_uuid$stringdist_pat1  <- ''
  different_uuid$stringdist_pat2  <- ''
  different_uuid$stringdist_pat3  <- ''
  different_uuid$obs  <- ''
  different_uuid$new_uuid  <- ''
  different_uuid$idart_full_name <- gsub(pattern = 'NA',replacement = "", x = paste0(different_uuid$firstnames, " ", different_uuid$lastname))
  different_uuid$idart_full_name <- gsub(pattern = '  ',replacement = " ", x = different_uuid$idart_full_name)
  jdbc_properties <- readJdbcProperties()
  
  for (k in 1:nrow(different_uuid)) {
    
    patient <- composePatientToCheck(k,different_uuid)
    
    # se a api nao retornar nada este nid nao existe
    df_openmrs_pat <- apiGetPatientByNid(jdbc.properties = jdbc_properties, patientid = patient)
    
    if(length(df_openmrs_pat$results)==0){
      
      message(paste0(" NID :", patient[2], " nao existe no openmrs"))
      different_uuid$obs[k]  <- paste0("NID :", patient[2], " nao existe no openmrs")
      
    }
    else  if(length(df_openmrs_pat$results)==1){
      
      different_uuid$duplicado_openmrs[k] <- 'No'
      if(different_uuid$uuididart[k] != df_openmrs_pat$results[[1]]$uuid){
        different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
        index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
        pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
        different_uuid$obs[k]  <- paste0("uuid must Change . Openmrs patient is:",pat_name_1)
      }

      
    } 
    else  if(length(df_openmrs_pat$results)==2){
      
      
      different_uuid$duplicado_openmrs[k] <- 'Yes'
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      
      different_uuid$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      different_uuid$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      
      if(different_uuid$stringdist_pat1[k] < different_uuid$stringdist_pat2[k]){
        
        different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
        different_uuid$obs[k] <- pat_name_1
        
        
      } else{
        
        different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[2]]$uuid
        different_uuid$obs[k] <- pat_name_2
      }
      
      
    }  else  if(length(df_openmrs_pat$results)==3){
      
      
      different_uuid$duplicado_openmrs[k] <- 'Yes'
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      index_t_name_start <- stri_locate_last(str =df_openmrs_pat$results[[3]]$display, regex = "-" )
      
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      pat_name_3 <- substr(df_openmrs_pat$results[[3]]$display, index_t_name_start+2, nchar(df_openmrs_pat$results[[3]]$display))
      
      different_uuid$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      different_uuid$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      different_uuid$stringdist_pat3[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_3)
      
      vec_str_dist <-c(different_uuid$stringdist_pat1[k],different_uuid$stringdist_pat2[k],different_uuid$stringdist_pat3[k]) 
      
      index = which(vec_str_dist == min(vec_str_dist))
      if(length(index)==1){
        
        if(index==1){
          different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
          different_uuid$obs[k] <- pat_name_1
        } else if(index==2){
            different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[2]]$uuid
            different_uuid$obs[k] <- pat_name_2
        }else if(index==3){
          different_uuid$new_uuid[k]  <- df_openmrs_pat$results[[3]]$uuid
          different_uuid$obs[k] <- pat_name_3
        }
        
        
      } else {  # pacientes duplicados nid nome no openmrs # resolver manualemnte
        
        different_uuid$obs[k] <- paste0("Paciente duplicados nid nome no openmrs, corrigir manualmente   NID:", patient[2])
      }
     
      
    }
    else{
      message(paste0(" Paciente 4x NID  no openmrs"))
      different_uuid$obs[k] <- paste0("Paciente com 4x  nid  no openmrs, corrigir manualmente NID:", patient[2])
    }
  }
  
  
  
}


temp_compare <- different_uuid %>% filter( new_uuid != "" & uuididart !=new_uuid )
temp_compare <- temp_compare %>% distinct(patientid ,.keep_all = TRUE)

# actualiza uuids correctos na BD idart
for( i in 1:nrow(temp_compare)){
  
  pat_id <- temp_compare$id[i]
  new_uuid <- temp_compare$new_uuid[i]
  base_query <- paste0(" update patient set uuidopenmrs ='",new_uuid,"' ,uuid ='", new_uuid, "' where id =",pat_id, " ;")
  print(base_query)
  write(base_query,file="update_querys.txt",append=TRUE)
}


nao_existe <- different_uuid %>% filter( grepl(pattern = "existe",x = obs,ignore.case = TRUE) ) 

if(nrow(nao_existe) > 0){
  
  nao_existe$match <- ""
  nao_existe$match_identifier <- ""
  nao_existe$uuid_openmrs <- ""
  nao_existe$match_identifier_1 <- ""
  nao_existe$uuid_openmrs_1 <- ""
  nao_existe$match_identifier_2 <- ""
  nao_existe$uuid_openmrs_2 <- ""
  nao_existe$duplicado_openmrs <- ""
  nao_existe$obs <- ""
  
  nao_existe$idart_full_name <- gsub(pattern = 'NA',replacement = "", x = paste0(nao_existe$firstnames, " ", nao_existe$lastname))
  nao_existe$idart_full_name <- gsub(pattern = '  ',replacement = " ", x = nao_existe$idart_full_name)
  nao_existe <- nao_existe[  , -which(names(nao_existe) %in% c("new_uuid","full_name_openmrs","uuidopenmrs","stringdist_pat1","stringdist_pat2","stringdist_pat3" ))]
  for (j in 1:nrow(nao_existe)) {
   
    patient_full_name <- nao_existe$idart_full_name[j]
    patient_full_name <- gsub(pattern = ' ', replacement = '%20',x =patient_full_name,ignore.case = TRUE )
    # se a api nao retornar nada este nid nao existe
    df_openmrs_pat <- apiGetPatientByName(jdbc_properties,patient_full_name )
    
    if(length(df_openmrs_pat$results)==0){
      
      message(paste0(" NID :", patient_full_name , " nao existe no openmrs"))
      nao_existe$match[j]  <- FALSE
      
    } 
    else  if(length(df_openmrs_pat$results)==1){
      
      nao_existe$match[j]  <- "yes"
      nao_existe$duplicado_openmrs[j] <- 'No'
      index_nid_start <- stri_locate_first(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      nid <- substr(df_openmrs_pat$results[[1]]$display, 1 ,  index_nid_start[1] -2 )
      uuid <- df_openmrs_pat$results[[1]]$uuid
      nao_existe$uuid_openmrs[j] <- uuid
      nao_existe$match_identifier[j] <- nid
      nao_existe$obs[j] <- "must change"
  }
    else  if(length(df_openmrs_pat$results)==2){
      
      
      index_nid <-  stri_locate_last(str =nao_existe$patientid[j], regex = "/" )
      if(tipo_nid =="Seq/Ano"){
        if(nchar(nao_existe$patientid[j])>8){
          
          seq <- substr(nao_existe$patientid[j]  ,index_nid[1]+1 , nchar(nao_existe$patientid[j]) )
        } else {
          seq <- substr(nao_existe$patientid[j]  ,1, index_nid[1]-1 )
        }
      
      } else {

          seq <- substr(nao_existe$patientid[j]  ,index_nid[1]+1 , nchar(nao_existe$patientid[j]) )
     
      }
   
      
      nao_existe$match[j]  <- "yes"
      nao_existe$duplicado_openmrs[j] <- 'yes'
      
      index_nid_1_start <- stri_locate_first(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_nid_2_start <- stri_locate_first(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      
      nid_1 <- substr(df_openmrs_pat$results[[1]]$display, 1 ,  index_nid_1_start[1] -2 )
      nid_2 <- substr(df_openmrs_pat$results[[2]]$display, 1 ,  index_nid_2_start[1] -2 )
      
      uuid_1 <- df_openmrs_pat$results[[1]]$uuid
      uuid_2 <- df_openmrs_pat$results[[2]]$uuid
      
      nao_existe$uuid_openmrs_1[j] <- uuid_1
      nao_existe$match_identifier_1[j] <- nid_1
      
      nao_existe$uuid_openmrs_2[j] <- uuid_2
      nao_existe$match_identifier_2[j] <- nid_2
      
      vec_nids <- c(nid_1,nid_2)
      index = which(grepl(pattern = seq, x = vec_nids))
      
      if(length(index)==1){
        
        if(index[1]==1){
          nao_existe$uuid_openmrs[j] <- uuid_1
          nao_existe$match_identifier[j] <- nid_1
          nao_existe$obs[j] <- "must change"

        } else if(index[1]==2){
          nao_existe$uuid_openmrs[j] <- uuid_2
          nao_existe$match_identifier[j] <- nid_2
          nao_existe$obs[j] <- "must change"
        }else{}
        
        
      } else {  # pacientes duplicados nid nome no openmrs # resolver manualemnte
        
        nao_existe$obs[k] <- paste0("Paciente duplicados nid nome no openmrs, corrigir manualmente   NID:", nao_existe$patientid[j])
      }
      
      
      
    }
    else if(length(df_openmrs_pat$results)>2){
    
    index_nid <-  stri_locate_last(str =nao_existe$patientid[j], regex = "/" )
    if(tipo_nid =="Seq/Ano"){
      if(nchar(nao_existe$patientid[j])>8){
        
        seq <- substr(nao_existe$patientid[j]  ,index_nid[1]+1 , nchar(nao_existe$patientid[j]) )
      } else {
        seq <- substr(nao_existe$patientid[j]  ,1, index_nid[1]-1 )
      }
      
    } else {
      
      seq <- substr(nao_existe$patientid[j]  ,index_nid[1]+1 , nchar(nao_existe$patientid[j]) )
      
    }
    

    nao_existe$match[j]  <- "yes"
    nao_existe$duplicado_openmrs[j] <- 'yes'
    # todos nids matched
    vec_nids <- c()
    
     for (v in 1:length(df_openmrs_pat$results)) {
       
       index_nid_start <- stri_locate_first(str =df_openmrs_pat$results[[v]]$display, regex = "-" )
       
       nid <- substr(df_openmrs_pat$results[[v]]$display, 1 ,  index_nid_start[1] -2 )
       
       vec_nids <- c(vec_nids,nid)
       
     }
    
    index = which(grepl(pattern = seq, x = vec_nids))
    if(length(index)==1){
      index_nid_openmrs<- stri_locate_first(df_openmrs_pat$results[[index[1]]]$display, regex = "-" )
      
      nid <- substr(df_openmrs_pat$results[[index[1]]]$display, 1 ,  index_nid_openmrs[1] -2 )
      uuid <- df_openmrs_pat$results[[index[1]]]$uuid
      nao_existe$uuid_openmrs[j] <- uuid
      nao_existe$match_identifier[j] <- nid
      nao_existe$obs[j] <- "must change"
      
    } else {  # pacientes duplicados nid nome no openmrs # resolver manualemnte
      
      nao_existe$obs[j] <- paste0("Paciente duplicados nid nome no openmrs, corrigir manualmente   NID:", nao_existe$patientid[j])
    }
    
    
    }
    else{message("do nothing")}
}
  
}

to_upate <- nao_existe %>% filter(obs=="must change")
# actualiza uuids & nids  correctos na BD idart
for( i in 1:nrow(to_upate)){
  
  pat_id <- to_upate$id[i]
  new_uuid <- to_upate$uuid_openmrs[i]
  new_patientid <- to_upate$match_identifier[i]
  
  first_query <- paste0("update patientidentifier set value = '",new_patientid, "' where patient_id = ", pat_id , " ;")
  
  base_query <- paste0(" update patient set uuidopenmrs ='",
                       new_uuid,
                       "' ,uuid ='",
                       new_uuid, 
                       "' , patientid = '",
                       new_patientid,
                       "' where id =",pat_id, " ;")
  
  print(base_query)
  write(first_query,file="update_querys.txt",append=TRUE)
  write(base_query,file="update_querys.txt",append=TRUE)
}
