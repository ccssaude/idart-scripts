

source('paramConfiguration.R')           ## Carregar as configuracoes






## patientsWithUuid
## Sao todos Pacientes no iDART que tem a coluna uuid preecnhida 
## uuid e' usado na versao idart ccs para fazer a sincronizacao , enquanto que a nova versao passa a usar a variavel uuidopenmrs
## patientsWithUuid <- subset(idartAllPatients, nchar(idartAllPatients$uuid)>0)

## patientWithDifferentUuid
## Sao  Pacientes  com a variavel uuid  diferente de uuidopenmrs  no iDART uuid!=uuidopenmrs
## Como Resolver?  E necessario analisar caso por caso
## TODO
patientWithDifferentUuid <- subset(idartAllPatients, idartAllPatients$uuid!=idartAllPatients$uuidopenmrs)
if(dim(patientWithDifferentUuid)[1]==0){
  rm(patientWithDifferentUuid)
} 
## patientsWoutUuid
## Sao todos Pacientes Sem uuid no iDART: uuid is null
patientsWoutUuid <-  subset(idartAllPatients, is.na(idartAllPatients$uuid) | nchar(idartAllPatients$uuid)==0,)

## patientsRefferedTransit
## Sao aqueles Pacientes Sem UUID  que sao : Transitos / Inicios na maternidade, Referidos
## Este grupo de pacientes geralmente nao existem no OpenMRS e por conseguinte nao precisam de uuid, vamos descartar
patientsRefferedTransit <-  subset(patientsWoutUuid, (patientsWoutUuid$startreason %in% c("Inicio na Maternidade","Paciente em Transito","Up Referred")) ,)

# Verifica se existe algum transito Paciente Transito no iDART Registado no OpenMRS
 length(which(patientsRefferedTransit$patientid %in% openmrsAllPatients$identifier))
 length(which(patientsRefferedTransit$uuid %in% openmrsAllPatients$uuid))
 
## Alguns pacientes tem a palavra Transito/Trans/T escrita no NID. Se existirem vamos juntar este grupo de pacientes com os pacientes referido  
##  no dataframe/tabela patientsRefferedTransit 
dfTemp <- patientsWoutUuid[which(grepl(pattern = "TR",ignore.case = TRUE,x=patientsWoutUuid$patientid)==TRUE),]

  
  if (dim(dfTemp)[1] != 0) { # verifica se a variavel tem dados, se tiver juntar/append/union
    
    patientsRefferedTransit <- rbind.fill(patientsRefferedTransit,dfTemp)
    rm(dfTemp)
  } else {    rm(dfTemp)}
  


## patientsWoutUuid
## Remover pacientes (Transitos / Inicios na maternidade, Referidos)  da  tabela/dataframe dos pacientes sem UUID 
## De modo a nos concentrar somente nos provaveis pacientes activos que nao tem uuid 
## Usei a a clausula not in ! %in%
patientsWoutUuid <-subset(patientsWoutUuid, !(patientsWoutUuid$id %in% patientsRefferedTransit$id),)  
rm(patientsRefferedTransit)


## Ja removemos pacientes em trasito do grupo de pacientes se UUID
## Aseguir veamos executar:
################### 1 - Verificar se temos algum Paciente do iDART sem uuid que esteja registado no OpenMRS    ###################
###################     Se existir comparar a informacao com base na semelhanca de nomes e actualizar          ###################
###################     o paciente no iDART                                                                    ###################
##################################################################################################################################


totalPacSemUuidRegOpenMRS <- length(which(patientsWoutUuid$patientid %in% openmrsAllPatients$identifier))


if(totalPacSemUuidRegOpenMRS>0){
  
  message(paste0("Temos ", totalPacSemUuidRegOpenMRS , " Pacientes iDART sem UUID  registados no OpenMRS"))
  
  
  ## pacSemUuidRegOpenMRS
  ## Sao Paciente do iDART sem uuid,  Registado no OpenMRS ( comparacao com base no NID)
  pacSemUuidRegOpenMRS <- patientsWoutUuid[which(patientsWoutUuid$patientid %in% openmrsAllPatients$identifier),]
  pacSemUuidRegOpenMRS <- pacSemUuidRegOpenMRS[, c(which(names(pacSemUuidRegOpenMRS)!='uuid'))] # retirar a coluna NID
  
  if(dim(pacSemUuidRegOpenMRS)[1]>0){
    # Cruzar a informacao destes pacientes com os pacientes na tabela openmrsAllPatients
    # de modo a obter o uuid e actualizar
    ## Actualizar o uuid destes pacientes no iDART
    pacSemUuidRegOpenMRS <- inner_join(pacSemUuidRegOpenMRS, openmrsAllPatients, c("patientid"="identifier"), keep=TRUE)
   
     #TODO - testar para numa US que  tenha casos de pacientes sem uuid mas o nid existe no OpenMRS
    for (v in 1:dim(pacSemUuidRegOpenMRS)[1]) {


      patient <- composePatientToLog(patientsWoutUuid,v)
      
       actualizaUuidIdart(con_openmrs,patient)

    }
    
    
    ## patientsWoutUuid
    ## Remover os pacientes encontrados na query anterior do dataframe pacSemUuidRegOpenMRS,
    ## De modo a usar outras tecnicas de verificacao 
    patientsWoutUuid <<- subset(patientsWoutUuid, !(patientsWoutUuid$id %in% pacSemUuidRegOpenMRS$id),)  
    
  }
   

  }
  


## Aseguir vemps executar:
################### 2 - Para cada paciente verificar o nr de total de dispensas. podemos assumir que   ###################
###################     os que tiverem totalDispensas <= 3  sao Transitos Mal registados              ###################
###################                                                                                    ###################
#########################################################################################################################


## Colocal o valor zero para pacientes sem levantamento
patientsWoutUuid$totaldispensas[which(is.na(patientsWoutUuid$totaldispensas))] <- 0

## patSemUuidProvaveisTransito
## Sao pacientes que nao tem uuid e tem menos de 3 Levantamentos, Sera que podemos assumir como Transitos?
## Descartamos estes Pacientes
## Remover os pacientes encontrados na query anterior(patSemUuidProvaveisTransito) do dataframe patientsWoutUuid,
## De modo a usar outras tecnicas de verificacao 
patSemUuidProvaveisTransito <- subset(patientsWoutUuid, patientsWoutUuid$totaldispensas<=3 )
patientsWoutUuid  <-subset(patientsWoutUuid, !(patientsWoutUuid$id %in% patSemUuidProvaveisTransito$id),) 
rm(patSemUuidProvaveisTransito)


################### 3 -Procurar os pacientes sem uuids no OpenMRS atraves de algoritmos de simetria de nomes    ###################
################### ( https://cran.r-project.org/web/packages/stringdist/stringdist.pdf ) Separar nome e sobrenome ###################
###################  dos pacientes do iDART, Pois no OpenMRS temos given_name, midle_name , family_name            # ################                                                                        ###################
###############################################################################################################################



patientsWoutUuid$full_name <- str_replace_na(paste0(patientsWoutUuid$firstnames, ' ',patientsWoutUuid$lastname),replacement=' ')

df_nomes_openmrs <- openmrsAllPatients[, c('uuid','patient_id','identifier','full_name_openmrs','estado_tarv','data_ult_levant')]
df_nomes_openmrs$distancia  <- ''


patientsWoutUuid$prov_openmrs_name  <- ''
patientsWoutUuid$prov_openmrs_uuid   <- ''
patientsWoutUuid$prov_openmrs_identifier  <- ''
patientsWoutUuid$prov_openmrs_patient_id  <- ''
patientsWoutUuid$prov_openmrs_estado_tarv  <- ''
patientsWoutUuid$prov_openmrs_data_ult_levant  <- ''
patientsWoutUuid$distancia  <- ''



for (i in 1:dim(patientsWoutUuid)[1]) {
  
  for( v in 1:dim(df_nomes_openmrs)[1]){
    
    df_nomes_openmrs$distancia[v] <- stringdist(patientsWoutUuid$full_name[i],df_nomes_openmrs$full_name_openmrs[v], method = "jw")
    
    
  }
  temp <- df_nomes_openmrs[which(df_nomes_openmrs$distancia == min(df_nomes_openmrs$distancia)),]
  
  patientsWoutUuid$prov_openmrs_name[i] <-  temp$full_name_openmrs[1]
  patientsWoutUuid$prov_openmrs_uuid [i] <-  temp$uuid[1]
  patientsWoutUuid$prov_openmrs_identifier[i] <-  temp$identifier[1]
  patientsWoutUuid$prov_openmrs_patient_id[i] <-  temp$patient_id[1]
  patientsWoutUuid$prov_openmrs_estado_tarv[i] <-  temp$estado_tarv[1]
  patientsWoutUuid$prov_openmrs_data_ult_levant[i] <-  temp$data_ult_levant[1]
  patientsWoutUuid$distancia[i] <-  temp$distancia[1] 

}




rm(df_nomes_openmrs)


