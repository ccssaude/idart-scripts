

library(RPostgreSQL)
#detach("package:RPostgreSQL", unload = TRUE)
con <-
  dbConnect(
    PostgreSQL(),
    user = "postgres",
    password = "postgres",
    dbname = "pharm",
    host = "192.168.100.100"
  )

no_uuid <- dbGetQuery(con, " SELECT pat.id, pat.accountstatus,  pat.firstnames,   pat.lastname,pat.patientid,pat.uuid, pat.uuidopenmrs, ep.startreason
  FROM patient pat left join episode ep on pat.id=ep.patient 
  where uuidopenmrs is null; ")

no_uuid_comp <- no_uuid[nchar(no_uuid$patientid)>15,]

no_uuid_comp$identifier <- ""
no_uuid_comp$uuid <- ""
no_uuid_comp$given_name <-""
no_uuid_comp$middle_name <-""
  no_uuid_comp$family_name  <-""


library(RMySQL)
openmrs = dbConnect(MySQL(), user='esaude', password='esaude', dbname='openmrs', host='192.168.100.100')

for (i in 1:dim(no_uuid_comp)[1]) {
  patientid = no_uuid_comp$patientid[i]
  rs <- dbSendQuery(openmrs, paste0("SELECT  p.identifier , pe.uuid, pn.given_name,pn.middle_name,pn.family_name
  FROM  patient pat INNER JOIN  openmrs.patient_identifier p ON pat.patient_id =p.patient_id
  INNER JOIN person pe ON pat.patient_id=pe.person_id
  INNER JOIN person_name pn ON pe.person_id=pn.person_id
  WHERE pe.uuid ='",patientid,"' ;"))
  
  data <- fetch(rs,n=1)  
  
  if(nrow(data)>0){
    no_uuid_comp$identifier[i] <- data$identifier[1]
    no_uuid_comp$uuid[i] <- data$uuid[1]
    no_uuid_comp$given_name[i] <- data$given_name[1]
    no_uuid_comp$middle_name[i] <- data$middle_name[1]
    no_uuid_comp$family_name[i]  <- data$family_name[1]
    
  }

  
  RMySQL::dbClearResult(rs)
  rm(data,rs)
  
}


no_uuid_comp <- no_uuid_comp[no_uuid_comp$patientid_openmrs!=0,]


for (i in 1:dim(no_uuid_comp)[1]) {
  nid = no_uuid_comp$patientid[i]
  rs <- dbSendQuery(openmrs, paste0("SELECT  p.identifier , pe.uuid, pn.given_name,pn.middle_name,pn.family_name
  FROM  patient pat INNER JOIN  openmrs.patient_identifier p ON pat.patient_id =p.patient_id
  INNER JOIN person pe ON pat.patient_id=pe.person_id
  INNER JOIN person_name pn ON pe.person_id=pn.person_id
WHERE p.identifier ='",nid,"' ;"))
 data <- fetch(rs,n=1) 
 if(nrow(data)>0){
 no_uuid_comp$identifier[i] <- data$identifier[1]
 no_uuid_comp$uuid[i] <- data$uuid[1]
 no_uuid_comp$given_name[i] <- data$given_name[1]
 no_uuid_comp$middle_name[i] <- data$middle_name[1]
 no_uuid_comp$family_name[i]  <- data$family_name[1]
}
 RMySQL::dbClearResult(rs)
 rm(data,rs)

}




############### nids da tabela patient id diferentes dos nids da tabela null_update_log

nids_diferente <-
  dbGetQuery(
    con,  paste0("  select pat.id, pat.patientid,pat.firstnames,pat.lastname,pat.uuidopenmrs, nul.original_nid,nul.new_nid, nul.uuid as nul_uuid
from patient pat inner join nid_update_log as nul on pat.uuidopenmrs = nul.uuid
where pat.patientid <> nul.original_nid")
  )


ni
#nids_diferente <-nids_diferente[c(-2),]


for (i in 434:dim(nids_diferente)[1]) {
  #patientid <- nids_diferente$patientid_openmrs[i]
  new_nid <- nids_diferente$original_nid[i]                                         
  id  <- nids_diferente$id[i]
  dbExecute(con,paste0("update  public.patientidentifier set value ='",new_nid,  "' where patient_id = ",  id, ";"   ))
  dbExecute(con,paste0("update  public.patient set patientid ='", new_nid,  "' where id = ",  id, ";"   ))
}


for (i in 3:dim(matched_by_names)[1]) {
  patientid <- matched_by_names$patientid_openmrs[i]
  new_nid <- matched_by_names$new_nid[i]                                         
  id  <- matched_by_names$id[i]
  #dbExecute(con,paste0("update  public.patientidentifier set value ='",new_nid,  "' where patient_id = ",  id,";"   ))
  dbExecute(con,paste0("update  public.patient set patientid ='", new_nid,  "' where id = ",  id," ;"   ))
}

################################################################################################



for (i in 1:dim(no_uuid_comp)[1]) {
  nid = no_uuid_comp$patientid[i]
  id =no_uuid_comp$patientid_openmrs[i]
  uuid =no_uuid_comp$uuid[i]
  
  dbExecute(
    con,
    paste0(
      "update public.patient set uuidopenmrs ='", uuid,"' where patientid_openmrs =",id," ; "
      
    )
  )
}
  





###  FIX uuid de duplicados

dups$new_uuid <- "" 
dups$name <- ""
dups$middle_name <- ""
dups$surname <- ""
################# get idart patient id

for (i in 1:dim(dups)[1]) {
  
  index <- dups$patientid_openmrs[i]

  pat <- dbSendQuery(openmrs, paste0("SELECT  pid.identifier , pe.uuid, pn.given_name,pn.middle_name,pn.family_name
  FROM  patient pat INNER JOIN  openmrs.patient_identifier pid ON pat.patient_id =pid.patient_id
  INNER JOIN person pe ON pat.patient_id=pe.person_id
  INNER JOIN person_name pn ON pe.person_id=pn.person_id
WHERE pat.patient_id = ",index," ;") )
  
  data <- fetch(pat,n=1)
  dups$new_uuid[i] <- data$uuid[1]
  dups$name[i] <- data$given_name[1]
  dups$middle_name[i] <- data$middle_name[1]
  dups$surname[i] <- data$family_name[1]
  RMySQL::dbClearResult(pat)
}



for (i in 1:dim(dups)[1]) {
  index <- dups$patientid_openmrs[i]
  new_uuid <- dups$new_uuid[i]
    dbExecute(con,  paste0(  "update public.patient set uuidopenmrs ='", new_uuid,"' where patientid_openmrs = ",index ,";"  )      )

    }
    
  










## Pacientes Duplicados Por NID
duplicadosOpenmrs <- getDuplicatesPatOpenMRS(con_openmrs)
duplicadosiDART <-   getDuplicatesPatiDART(con_postgres)


# Cruzar com dados do openmrs
dups_idart_openmrs <- inner_join(duplicadosiDART,openmrsAllPatients, by=c("uuid"))
dups_idart_openmrs$solucao <- ""

## Para cada grupo de duplicados (NID) procurar solucao

############################################ Grupo 1 (G1): Pacientes  duplicados com mesmo uuid    ########################################## 

## Grupo   1.1 (G1.1):  -Pacientes  duplicados no iDART e no OpenMRS ,e  tem o mesmo uuid
## Solucao G1.1:  Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados

## Grupo   1.2 (G1.2):  -Pacientes  duplicados apenas iDART ,  e tem o mesmo uuid
## Solucao G1.2:   Unir  os paciente no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados


###########################################  Grupo 2 (G2): Pacientes  duplicados  com  uuids diferentes e nomes diferentes ############ 

## Grupo   2.1 (G2.1):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao abandonos 
## Solucao G2.1:   Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: aquele que tiver a data do ult_lev menos recente

## Grupo   2.2 (G2.2):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao activos
## Solucao G2.2:   Trocar o nid de um dos pacientes, os dois estao activos, verificar processos clinicos

## Grupo   2.3 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
## Solucao (G2.3) - Trocar o nid do paciente que nao e activo

## Grupo    2.4 (G2.4):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
## Solucao (G2.4) - Trocar o nid do paciente que nao e activo


################################  Grupo 3 (G3): Pacientes  duplicados  com uuids diferentes e nomes semelhantes  ( Algoritmo de simetria de Nomes)############ 
## Algoritimo  de simetria de strings com o method :  Jaro-Winker distance ver 
## https://cran.r-project.org/web/packages/stringdist/stringdist.pdf pag 19 & https://pt.wikipedia.org/wiki/Dist%C3%A2ncia_de_Jaro-Winkler

## Grupo   3.1 (G3.1):  -Pacientes  duplicados no iDART e no OpenMRS , com nomes semelhantes
## Solucao 3.1 (G3.1):  Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados



nidsAllDupsPatients <- unique(dups_idart_openmrs$patientid)

for (i in 1:length(nidsAllDupsPatients)) {
  nid_duplicado <- nidsAllDupsPatients[i]
  
  index <- which(dups_idart_openmrs$patientid==nid_duplicado)
  df_temp <- dups_idart_openmrs[index,]
  
  if(length(index)==2){
    
    if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){  ## Grupo 1 (G1)
      
      # Verificar se e duplicado no OpenMRS 
      if(dups_idart_openmrs$uuid[index[1]] %in% duplicadosOpenmrs$uuid){
        # Solucao Grupo 1 (G1)
        solucao <- "  Unir  os paciente no iDART e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados"
        SetSolucao(index,solucao)
      } else
      {
        # nao e duplicado no OpenMRS
        # Solucao Grupo 1.2 (G1.2)
        solucao <- "  Unir  os paciente no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados"
        SetSolucao(index,solucao)
      }
      
      
    } else { ## Grupo 2 (G2)
      #  avaliar o grau de simetria dos nomes , para saber se trata-se do mesmo paciente 
      
      nome_pat_1 <- dups_idart_openmrs$full_name[index[1]]
      nome_pat_2 <- dups_idart_openmrs$full_name[index[2]]
      estado_tarv_1 <- dups_idart_openmrs$estado_tarv[index[1]]
      estado_tarv_2 <- dups_idart_openmrs$estado_tarv[index[2]]
      
      if(stringdist(nome_pat_1,nome_pat_2, method = "jw") > 0.1){     # Grupo 2 (G2)
        #s e o rsultado de stringdist for:
        # 0 - perfect match, 0,1 - minimo aceitavel definido por mim, 1 - no match at al
        
        if(estado_tarv_1 == estado_tarv_2 & estado_tarv_2=='ABANDONO'){ # Grupo 2.1 (G2.1) Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao abandonos 
          
          
          pac_lev_menos_rec <- getPacLevMenosRecente(index,nid_duplicado)
          solucao <- paste0("Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", pac_lev_menos_rec, " por ter a data do ult lev menos recente")
          SetSolucao(index,solucao)
          
        }
        if(estado_tarv_1 != estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){
          
          if( 'TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){  # Solucao (G2.2) - Os dois pacientes estao activos em tarv
            
            solucao <- paste0("Trocar o nid de um dos pacientes, os dois estao activos, verificar processos clinicos") 
            SetSolucao(index,solucao)
            
          } else{
            
            
            if(estado_tarv_1== 'ACTIVO NO PROGRAMA'){  # Grupo 2.2 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
              
              index <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
              nome_pat_nao_activo <- dups_idart_openmrs$full_name[index]
              
              solucao <- paste0("Trocar o nid do paciente: ", nome_pat_nao_activo, " ele nao e activo em tarv")
              SetSolucao(index,solucao)
            }
            
            
          }
          
          pac_lev_menos_rec <- getPacLevMenosRecente(index,nid_duplicado)
          # Solucao Grupo 2 (G2)
          solucao <- paste0("Trocar o nid de um dos pacientes, de preferencia: ", pac_lev_menos_rec, " Por ter a data do ult lev menos recente")
          SetSolucao(index,solucao)
        } else {   
          
          
          # Solucao (G2.2) - Os dois pacientes estao activos em tarv
          solucao <- paste0("Trocar o nid de um dos pacientes, os dois estao activos, verificar processos clinicos") 
          SetSolucao(index,solucao)
        }
        
      }
      
      else {  #  Nomes sao semelhantes , podemos assumir que trata-se do mesmo pacientes
        
        # Verficar se sao duplicados no openmrs 
        ## Grupo   3.1 (G3.1):  -Pacientes  duplicados no iDART e no OpenMRS , com nomes semelhantes
        # priorizar o que tiver levantamentos mais actualizados
        if(nid_duplicado %in% duplicadosOpenmrs$Nid){
          
          nome_lev_mais_act <- getPacLevMaisRecente(index,nid_duplicado)
          # Solucao Grupo 3.1 (G3.1)
          
          solucao <- paste0("Unir  os paciente ",nome_pat_1," e ",nome_pat_2, " no iDART e no OpenMRS. Paciente com nome: ",
                            nome_lev_mais_act, " e preferido por ter a data do ult lev mais recente")
          SetSolucao(index,solucao)
          
        } else {
          
          
          solucao <- paste0("Unir  os paciente  no iDART . O pac preferido e aquele que tiver data do ult lev mais recente")
          SetSolucao(index,solucao)
        }
        
      }
      
    }
  }
}
dups_idart_openmrs$solucao[index[1]] <- solucao
dups_idart_openmrs$solucao[index[2]] <- solucao
