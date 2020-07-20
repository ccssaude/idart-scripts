# pacientes com mesmo nid mas uuid e diferente idartuuid != personuuid
library(properties)
library(httr)

library(tibble)
library(writexl)
# Verifica e actualiza pacientes que tem dados no iDART diferentes do OpenMRS
# --  cidalia joao
# ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

wd <- '~/R/iDART/idart-scripts/dataClean/'

# Limpar o envinronment
rm(list=setdiff(ls(), c("wd","tipo_nid")))

if (dir.exists(wd)){
  
  setwd(wd)  
  source('paramConfiguration.R')     
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}

####################### diferenca de uuids entre pacientes   (uuid do idart e diferente uuid openmrs  mas os nids sao iguais) ###############################
#############################################################################################################################################################

temp <- getPatientsInvestigar(con_openmrs)
idartAllPatients <- getAllPatientsIdart(con_postgres)
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

# pacientes activos com uuid vazio na tabela patient
empty_uuid_patient <- different_uuid %>% filter(is.na(uuididart)) %>% select(id, patientid,firstnames,lastname, uuididart,uuidopenmrs, full_name_openmrs)

different_uuid_check <- different_uuid  %>% filter(!is.na(uuididart)) %>% select(id, patientid,firstnames,lastname, uuididart,uuidopenmrs, full_name_openmrs)

if(nrow(different_uuid_check)>0){
  different_uuid_check <- different_uuid_check[!is.na(different_uuid_check$patientid),]
  different_uuid_check$duplicado_openmrs <- ''
  different_uuid_check$stringdist_pat1  <- ''
  different_uuid_check$stringdist_pat2  <- ''
  different_uuid_check$stringdist_pat3  <- ''
  different_uuid_check$obs  <- ''
  different_uuid_check$new_uuid  <- ''
  different_uuid_check$idart_full_name <- gsub(pattern = 'NA',replacement = "", x = paste0(different_uuid_check$firstnames, " ", different_uuid_check$lastname))
  different_uuid_check$idart_full_name <- gsub(pattern = '  ',replacement = " ", x = different_uuid_check$idart_full_name)
  jdbc_properties <- readJdbcProperties()
  
  for (k in 1:nrow(different_uuid_check)) {
    
    patient <- composePatientToCheck(k,different_uuid_check)
    
    # se a api nao retornar nada este nid nao existe
    df_openmrs_pat <- apiGetPatientByNid(jdbc.properties = jdbc_properties, patientid = patient)
    
    if(names(df_openmrs_pat)=="error"){
      if(df_openmrs_pat$error$message=="User is not logged in"){
        stop(" o user da api nao foi criado  authenticate('farmac', 'iD@rt2020!') ")
      }
      
    }
    
    if(length(df_openmrs_pat$results)==0){
      
      message(paste0(" NID :", patient[2], " nao existe no openmrs"))
      different_uuid_check$obs[k]  <- paste0("NID :", patient[2], " nao existe no openmrs")
      
    }
    else  if(length(df_openmrs_pat$results)==1){
      
      different_uuid_check$duplicado_openmrs[k] <- 'No'
      if(different_uuid_check$uuididart[k] != df_openmrs_pat$results[[1]]$uuid){
        different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
        index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
        pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
        different_uuid_check$obs[k]  <- paste0("uuid must Change . Openmrs patient is:",pat_name_1)
      }

      
    } 
    else  if(length(df_openmrs_pat$results)==2){
      
      
      different_uuid_check$duplicado_openmrs[k] <- 'Yes'
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      
      different_uuid_check$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      different_uuid_check$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      
      vec <- c(different_uuid_check$stringdist_pat1[k],different_uuid_check$stringdist_pat2[k])
      
      index <- which( vec == min(vec) )
      
      if(index==2){
          
        
          different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[2]]$uuid
          different_uuid_check$obs[k] <- pat_name_2
        
       }else{
        
         
         different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
         different_uuid_check$obs[k] <- pat_name_1
           
        }
     
      
    
      
      
    }  else  if(length(df_openmrs_pat$results)==3){
      
      
      different_uuid_check$duplicado_openmrs[k] <- 'Yes'
      index_f_name_start <- stri_locate_last(str =df_openmrs_pat$results[[1]]$display, regex = "-" )
      index_s_name_start <- stri_locate_last(str =df_openmrs_pat$results[[2]]$display, regex = "-" )
      index_t_name_start <- stri_locate_last(str =df_openmrs_pat$results[[3]]$display, regex = "-" )
      
      pat_name_1 <- substr(df_openmrs_pat$results[[1]]$display, index_f_name_start+2, nchar(df_openmrs_pat$results[[1]]$display))
      pat_name_2 <- substr(df_openmrs_pat$results[[2]]$display, index_s_name_start+2, nchar(df_openmrs_pat$results[[2]]$display))
      pat_name_3 <- substr(df_openmrs_pat$results[[3]]$display, index_t_name_start+2, nchar(df_openmrs_pat$results[[3]]$display))
      
      different_uuid_check$stringdist_pat1[k] <- getStringDistance(string1 = patient[5],string2 = pat_name_1)
      different_uuid_check$stringdist_pat2[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_2)
      different_uuid_check$stringdist_pat3[k] <- getStringDistance(string1 = patient[5], string2 = pat_name_3)
      
      vec_str_dist <-c(different_uuid_check$stringdist_pat1[k],different_uuid_check$stringdist_pat2[k],different_uuid_check$stringdist_pat3[k]) 
      
      index = which(vec_str_dist == min(vec_str_dist))
      if(length(index)==1){
        
        if(index==1){
          different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[1]]$uuid
          different_uuid_check$obs[k] <- pat_name_1
        } else if(index==2){
            different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[2]]$uuid
            different_uuid_check$obs[k] <- pat_name_2
        }else if(index==3){
          different_uuid_check$new_uuid[k]  <- df_openmrs_pat$results[[3]]$uuid
          different_uuid_check$obs[k] <- pat_name_3
        }
        
        
      } else {  # pacientes duplicados nid nome no openmrs # resolver manualemnte
        
        different_uuid_check$obs[k] <- paste0("Paciente duplicados nid nome no openmrs, corrigir manualmente   NID:", patient[2])
      }
     
      
    }
    else{
      message(paste0(" Paciente 4x NID  no openmrs"))
      different_uuid_check$obs[k] <- paste0("Paciente com 4x  nid  no openmrs, corrigir manualmente NID:", patient[2])
    }
  }
  
  
  
}


## nids que estao no idart que nao existem no openmrs
no_existe_openmrs <- filter(different_uuid_check, grepl(pattern = 'nao existe no openmrs',x = obs,ignore.case = 'TRUE'))

# Nids com uuids no idart != uuid do OpenMRS
temp_compare <- filter(different_uuid_check, !grepl(pattern = 'nao existe no openmrs',x = obs,ignore.case = 'TRUE') )
temp_compare <- temp_compare %>% filter(new_uuid != "" & uuididart !=new_uuid)
temp_compare <- temp_compare %>% distinct( patientid ,.keep_all = TRUE )


# preparar um script para corrigir uuids
for( i in 1:nrow(temp_compare)){

  if(grepl(pattern = 'uuid must Change',x =  temp_compare$obs[i] , ignore.case = TRUE)){
    pat_id <- temp_compare$id[i]
    new_uuid <- temp_compare$new_uuid[i]
    
    
    base_query <- paste0(" update patient set uuidopenmrs ='",
                         new_uuid,
                         "' ,uuid ='",
                         new_uuid,
                         "' where id =",pat_id, " ;")
    
    print(base_query)
    write(base_query,file="fix_uuid_querys.txt",append=TRUE)
    
  }
  
}

# recolhe informacao dos duplicados por nid no openmrs
duplicados_corrigir_manualmente <- temp_compare %>%  filter ( duplicado_openmrs == "Yes") %>% select(id, patientid,firstnames,lastname, uuididart,
                                                           uuidopenmrs, full_name_openmrs, duplicado_openmrs)
duplicados_corrigir_manualmente$uuidopenmrs <- ""
#Gravar um excell e partilhar com digitacao/farmacia para resolverem
#Deve-se preencher a coluna novo_uuid com o uuid do paciente que vai permanecer no openmrs

write_xlsx(x = duplicados_corrigir_manualmente, path = paste0('output/', gsub(pattern = ' ', replacement = "_", x = us.name), '_duplicados_openrms_manualmente.xlsx'))
write_xlsx(x = no_existe_openmrs, path = paste0('output/', gsub(pattern = ' ', replacement = "_", x = us.name), '_nids_idart_no_exist_openmrs.xlsx'))


