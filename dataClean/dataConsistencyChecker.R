# Packages que contem algumas funcoes a serem usadas 
require(RMySQL)
require(plyr)    ## Este package contem a funcao rbind.fill para fazer union all de 2 dataframes (tabelas)
require(stringi)
require(RPostgreSQL)
require(stringr)
require(tidyr)
require(stringdist)
require(dplyr)  ## Este package contem a funcao inner_join

####################################### Configuracao de Parametros  #####################################################################
#########################################################################################################################################
wd <- '~/R/iDART/ccs/dataClean/'  ## dConfigure para qualquer directorio , e guardar o ficheiro helperFunctions.R neste dir
setwd(wd)
source('helperFunctions.R')  ## Carregar funcoes


## OpenMRS Stuff - Configuracoes de variaveis de conexao 
openmrs.user ='esaude'
openmrs.password='esaude'
openmrs.db.name='1maio'
openmrs.host='127.0.0.2'
openmrs.port=3333
us.code= '0111030701' # modificar este parametro para cada US. Este e o Cod da US definido pelo MISAU e geralmente e a primeira parte do NID
# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)


# iDART Stuff - Configuracoes de variaveis de conexao 
postgres.user ='postgres'
postgres.password='postgres'
postgres.db.name='maio'
postgres.host='127.0.0.3'
postgres.port=5432
# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
############################################################################################################################################

## Pacientes
## Buscar todos pacientes OpenMRS & iDART
openmrsAllPatients <- getAllPatientsOpenMRS(con_openmrs)
idartAllPatients <- getAllPatientsIdart(con_postgres)


## patientsWithUuid
## Sao todos Pacientes no iDART que tem a coluna uuid preecnhida 
## uuid e' usado na versao idart ccs para fazer a sincronizacao , enquanto que a nova versao passa a usar a variavel uuidopenmrs
## patientsWithUuid <- subset(idartAllPatients, nchar(idartAllPatients$uuid)>0)

## patientWithDifferentUuid
## Sao  Pacientes  com a variavel uuid  diferente de uuidopenmrs  no iDART uuid!=uuidopenmrs
## Como Resolver?  E necessario analisar caso por caso
## TODO
patientWithDifferentUuid <- subset(idartAllPatients, idartAllPatients$uuid!=idartAllPatients$uuidopenmrs)

## patientsWoutUuid
## Sao todos Pacientes Sem uuid no iDART: uuid is null
patientsWoutUuid <-  subset(idartAllPatients, is.na(idartAllPatients$uuid) | nchar(idartAllPatients$uuid)==0,)

## patientsRefferedTransit
## Sao aqueles Pacientes Sem UUID  que sao : Transitos / Inicios na maternidade, Referidos
## Este grupo de pacientes geralmente nao existem no OpenMRS e por conseguinte nao precisam de uuid, vamos descartar
patientsRefferedTransit <-  subset(patientsWoutUuid, (patientsWoutUuid$startreason %in% c("Inicio na Maternidade","Paciente em Transito","Up Referred")) ,)

# Verifica se existe algum transito Paciente Transito no iDART Registado no OpenMRS
 length(which(patientsRefferedTransit$patientid %in% openmrsAllPatients$identifier))

## Alguns pacientes tem a palavra Transito/Trans/T escrita no NID. Se existirem vamos juntar este grupo de pacientes com os pacientes referido  
##  no dataframe/tabela patientsRefferedTransit 
dfTemp <- patientsWoutUuid[which(grepl(pattern = "TRA",ignore.case = TRUE,x=patientsWoutUuid$patientid)==TRUE),]

if (! is.null(dfTemp) ) { # verifica se o script em cima retorou algo 
  
  if (dim(dfTemp)[1] != 0) { # verifica se a variavel tem dados, se tiver juntar/append/union
    
    patientsRefferedTransit <- rbind.fill(patientsRefferedTransit,dfTemp)
    rm(dfTemp)
  }
  
}


## patientsWoutUuid
## Remover pacientes (Transitos / Inicios na maternidade, Referidos)  da  tabela/dataframe dos pacientes sem UUID 
## De modo a nos concentrar somente nos provaveis pacientes activos que nao tem uuid 
## Usei a a clausula not in ! %in%
patientsWoutUuid <-subset(patientsWoutUuid, !(patientsWoutUuid$id %in% patientsRefferedTransit$id),)  



## Ja removemos pacientes em trasito do grupo de pacientes se UUID
## Aseguir veamos executar:
################### 1 - Verificar se temos algum Paciente do iDART sem uuid que esteja registado no OpenMRS    ###################
###################     Se existir comparar a informacao com base na semelhanca de nomes e actualizar          ###################
###################     o paciente no iDART                                                                    ###################
##################################################################################################################################


totalPacSemUuidRegOpenMRS <- length(which(patientsWoutUuid$patientid %in% openmrsAllPatients$identifier))
if(totalPacSemUuidRegOpenMRS>0){
  
  print(paste0("Temos ", totalPacSemUuidRegOpenMRS , " Pacientes iDART sem UUID  registados no OpenMRS"))
  
}


## pacSemUuidRegOpenMRS
## Sao Paciente do iDART sem uuid,  Registado no OpenMRS ( comparacao com base no NID)
pacSemUuidRegOpenMRS <- patientsWoutUuid[which(patientsWoutUuid$patientid %in% openmrsAllPatients$identifier),]
pacSemUuidRegOpenMRS <- inner_join(pacSemUuidRegOpenMRS, openmrsAllPatients, c("patientid"="identifier"), keep=TRUE)

## Actualizar o uuid destes pacientes no iDART, antes verifique o dataframe ha casos de pacientes com mesmo NID e nomes diferentes
## nao incluir estes pacientes na actualizacao a seguir

for (i in 1:dim(pacSemUuidRegOpenMRS)[1]) {
  ## TODO
  
}


## patientsWoutUuid
## Remover os pacientes encontrados na query anterior do dataframe patientsWoutUuid,
## De modo a usar outras tecnicas de verificacao 
patientsWoutUuid <-subset(patientsWoutUuid, !(patientsWoutUuid$id %in% pacSemUuidRegOpenMRS$id),)  


## Aseguir vemps executar:
################### 2 - Para cada paciente verificar o nr de total de dispensas. podemos assumir que   ###################
###################     os que tiverem totalDispensas <= 3  sao Transitos Mal registados              ###################
###################                                                                                    ###################
#########################################################################################################################



patientsWoutUuid$totalLevantamentos <-""
patientsWoutUuid$totalLevantamentos <- sapply(patientsWoutUuid$id,getTotalDeDispensasPorPaciente)
## Colocal o valor zero para pacientes sem levantamento
patientsWoutUuid$totalLevantamentos[which(is.na(patientsWoutUuid$totalLevantamentos))] <- 0

## patSemUuidProvaveisTransito
## Sao pacientes que nao tem uuid e tem menos de 3 Levantamentos, Sera que podemos assumir como Transitos?
## Descartamos estes Pacientes
## Remover os pacientes encontrados na query anterior(patSemUuidProvaveisTransito) do dataframe patientsWoutUuid,
## De modo a usar outras tecnicas de verificacao 
patSemUuidProvaveisTransito <- subset(patientsWoutUuid, patientsWoutUuid$totalLevantamentos<=3 )
patientsWoutUuid  <-subset(patientsWoutUuid, !(patientsWoutUuid$id %in% patSemUuidProvaveisTransito$id),) 



################### 3 -Procurar os pacientes sem uuids no OpenMRS atraves de algoritmos de simetria de nomes    ###################
################### ( https://cran.r-project.org/web/packages/stringdist/stringdist.pdf ) Separar nome e sobrenome ###################
###################  dos pacientes do iDART, Pois no OpenMRS temos given_name, midle_name , family_name            # ################                                                                        ###################
###############################################################################################################################

## O nome mais proximo encotrado no openmrs aplicando algoritmo de simetria de nomes

## dataframe temporario 
temp_df <- fillFullName(patientsWoutUuid) 

for(i in 1:dim(temp_df)[1]){
  
  nome_idart <- temp_df$full_name[i]
  
  ## Pacientes no openMRScom o nome mais prox. de nome_idart
  ## Aplicando o algoritimo  de simetria de strings com o method :  Jaro-Winker distance ver  https://cran.r-project.org/web/packages/stringdist/stringdist.pdf pag 19 & https://pt.wikipedia.org/wiki/Dist%C3%A2ncia_de_Jaro-Winkler
  dist_minima <- min(stringdist(nome_idart, openmrsAllPatients$full_name, method = "jw"))
  prox_matched <- openmrsAllPatients[which(stringdist(nome_idart, openmrsAllPatients$full_name, method = "jw")==dist_minima),]
  temp_df$nome_aprox_openmrs[i] <- prox_matched$full_name[1]
  temp_df$nid_nome_aprox_openmrs[i] <- prox_matched$identifier[1]
  temp_df$birthdate_openmrs[i] <- prox_matched$birthdate[1]
  temp_df$dist_de_aproximacao[i] <- round(as.numeric(dist_minima),digits = 4)
  temp_df$uuid_openmrs[i] <- prox_matched$uuid[1]
  temp_df$estado_tarv_openmrs[i] <- prox_matched$estado_tarv[1]
  
}





patientsWoutUuid$patientid <- sapply(patientsWoutUuid$patientid,removeLettersFromNid)

