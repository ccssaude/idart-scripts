# Packages que contem algumas funcoes a serem usadas 
require(RMySQL)
require(RPostgreSQL)
require(openssl)  ## instalar com install.packages("openssl")
require(plyr)     ## instalar com install.packages("plyr")
require(dplyr)    ## instalar com install.packages("dplyr")
require(readr)    ## instalar com install.packages("readr")
####################################### Configuracao de Parametros  #####################################################################
#########################################################################################################################################
wd <- '~/R/iDART/idart-scripts/upgrade/'  
setwd(wd)
source('sql_querys.R')
source('genericFunctions.R')

## OpenMRS Stuff - Configuracoes de variaveis de conexao 
openmrs.user ='esaude'
openmrs.password='esaude'
openmrs.db.name='junho'
openmrs.host='172.17.0.2'
openmrs.port=3306
#us.code= '0111040601' # CS 1 junho# modificar este parametro para cada US. Este e o Cod da US definido pelo MISAU e geralmente e a primeira parte do NID
# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)


# iDART Stuff - Configuracoes de variaveis de conexao 
postgres.user ='postgres'
postgres.password='postgres'
postgres.db.name='junho'
postgres.host='172.17.0.4'
postgres.port=5432
# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
############################################################################################################################################

## 1 --  Primeiro executar o script com alteracoes atraves do pg admin

# Deve-se mudar o nome de Clinic name , StockCenter name no iDART de modo a ser igual ao default_location  no openmrs
new_clinic_name  <- getOpenmrsDefaultLocation(con_openmrs)
idart_clinic_name <- getIdartClinicName(con_postgres)
facility_name <- getIdartFacilityName(con_postgres)
stockcenter_name <- getIdartStockCenterName(con_postgres)



## Update facility name , stockcenter name clinic name
status <- dbExecute(  con_postgres,  paste0("  update public.nationalclinics set facilityname = '",new_clinic_name,"' where facilityname = '",idart_clinic_name,"';"))
if(status>1){
  message('facilityname Actualizado com Sucesso')
  }else {
  message('Nao foi possivel inserir o facilityname no iDART Insira manualmente')
  
}


status <- dbExecute(con_postgres,  paste0("update public.clinic    set clinicname = '",new_clinic_name,"' where clinicname = '",facility_name,"';"))
if(status>1){
  message('clinicname Actualizado com Sucesso')
}else {
  message('Nao foi possivel inserir o clinicname no iDART Insira manualmente')
  
}
 

status <-  dbExecute(  con_postgres,  paste0("update public.stockcenter    set stockcentername = '",new_clinic_name,"' where stockcentername = '",stockcenter_name,"';"))
if(status>1){
  message('stockcentername Actualizado com Sucesso')
}else {
  message('Nao foi possivel inserir o stockcentername no iDART Insira manualmente')
  
}
# Actualiza uuidopenmrs para ser igual a uuid (set uuidopenmrs=uuid)
dbExecute(con_postgres, sql_update_uuid_openmrs)

# Actualiza Nids da tabela patientidentifier de modo a serem iguais aos Nids do Patient  
dbExecute(con_postgres, sql_update_nid_patient_identifier)

#   Buscar os regimes Terapeuticos na BD iDART
regimes_terap <- getRegimesTerapeuticos(con_postgres)

### Carrega a tabela/dataframe dos  Regimes Padronizados 
load(file = 'regimes.RDATA')

## Reggimes no estado active=FALSE nao nos interessam


regimes_terap <- regimes_terap[regimes_terap$active == TRUE,  ]
regimes_terap <- regimes_terap[order(regimes_terap$regimeesquema), ]



## Compara os regimes existentes com os regimes  Padronizados e faz actualizacao.
## Se o regime nao exite no iDART Faz-se uma insercao

for (i in 1:dim(regimes_padronizados)[1]) {
  
  regime <- regimes_padronizados$regimeesquema[i]
  found <- FALSE
  for (v in 1:dim(regimes_terap)[1]) {
    
    if (regime == regimes_terap$regimeesquema[v])
    {
      regimeid <- regimes_terap$regimeid[v]
      cod_regime <- regimes_padronizados$codigoregime[i]
      regimenomeespecificado <- regimes_padronizados$regimenomeespecificado[i]
    
      actualizaRegimeTerapeutico(con_postgres,regimeid,cod_regime,regimenomeespecificado)
      
      found <-TRUE
      break
      
    }
  }
  if(!found){
    regimeid <- regimes_padronizados$regimeid[i]
    cod_regime <- regimes_padronizados$codigoregime[i]
    regime_esquema <- regimes_padronizados$regimeesquema[i]
    regimenomeespecificado <- regimes_padronizados$regimenomeespecificado[i]
    
    insertRegimeTerapeutico(con_postgres,regimeid , regime_esquema,cod_regime,active = TRUE,  regimenomeespecificado)
    
  }

  
}


## Apos padronizacao desactiva regimes sem uuid
dbExecute(con_postgres,sql_desactiva_regimes_sem_uuid)

## iDART exige que tods regimes activos tenham o campo  codregime,
## este cog garante nehum regim tenha codrigme vazio ( situacoes raras)


dbExecute(con_postgres,sql_update_cod_regime_null )
dbExecute(con_postgres,sql_update_admin_md5 )



insertGenericProvider(con_postgres)

