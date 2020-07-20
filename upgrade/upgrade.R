# Packages que contem algumas funcoes a serem usadas 
require(RMySQL)
require(RPostgreSQL)
require(plyr)     ## instalar com install.packages("plyr")
require(dplyr)    ## instalar com install.packages("dplyr")
####################################### Configuracao de Parametros  #####################################################################
#########################################################################################################################################
wd <- '~/R/iDART/idart-scripts/upgrade/'  
setwd(wd)

source('sql_querys.R')
source('genericFunctions.R')

# iDART Stuff - Configuracoes de variaveis de conexao 
postgres.user ='postgres'
postgres.password='postgres'
postgres.db.name='xipamanine'
postgres.host='172.18.0.3'
postgres.port=5432
# Objecto de connexao com a bd openmrs postgreSQL
con_local <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
############################################################################################################################################


## OpenMRS Stuff - Configuracoes de variaveis de conexao 
openmrs.user ='esaude'
openmrs.password='esaude'
openmrs.db.name='openmrs'
openmrs.host='192.168.0.150'
openmrs.port=3306
#us.code= '0111040601' # CS 1 junho# modificar este parametro para cada US. Este e o Cod da US definido pelo MISAU e geralmente e a primeira parte do NID
# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)

#   Buscar os regimes Terapeuticos na BD iDART
regimes_terap <- getRegimesTerapeuticos(con_local)

#Carregar os regimes padronizados
load(file = 'regimes.RDATA')

#regimes_padronizados <- dbGetQuery(con_local, 'select * from regimeterapeutico where active=TRUE ;')
# regimes_padronizados$regimenomeespecificado <- gsub(' ',replacement = '',x =regimes_padronizados$regimenomeespecificado )
# regimes_padronizados$regimenomeespecificado <- gsub('\r\n',replacement = '',x =regimes_padronizados$regimenomeespecificado )


#regimes_terap <- regimes_terap[regimes_terap$active == TRUE,  ]
#regimes_padronizados <- regimes_padronizados[regimes_padronizados$active == TRUE,  ]

regimes_terap <- regimes_terap[order(regimes_terap$regimeesquema), ]
regimes_padronizados <- regimes_padronizados[order(regimes_padronizados$regimeesquema), ]


## Compara os regimes existentes com os regimes  Padronizados e faz actualizacao.
## Se o regime nao exite no iDART Faz-se uma insercao

for (i in 1:dim(regimes_padronizados)[1]) {
  id <- regimes_padronizados$regimeid[i]
  found <- FALSE
  for (v in 1:dim(regimes_terap)[1]) {
    if (id == regimes_terap$regimeid[v]) {
      cod_regime <- regimes_padronizados$codigoregime[i]
      regimenomeespecificado <-
        regimes_padronizados$regimenomeespecificado[i]
      regime_esquema <- regimes_padronizados$regimeesquema[i]
      
      dbExecute(
        con_local,
        paste0(
          "update public.regimeterapeutico set regimenomeespecificado ='",
          regimenomeespecificado,
          "'",
          " , codigoregime = '",
          cod_regime,
          "', regimeesquemaidart = '",
          regime_esquema,
          "' , regimeesquema = '",
          regime_esquema,
          "' , active =TRUE ",
          " where regimeid = ",
          id,
          " ;"
        )
      )
      
      found <- TRUE
      break
      
    }
  }
  if (!found) {
    regime <- regimes_padronizados$regimeesquema[i]
    for (v in 1:dim(regimes_terap)[1]) {
      if (regime == regimes_terap$regimeesquema[v]) {
        cod_regime <- regimes_padronizados$codigoregime[i]
        regimenomeespecificado <- regimes_padronizados$regimenomeespecificado[i]
        id <- regimes_terap$regimeid[v]
        dbExecute(
          con_local,
          paste0(
            "update public.regimeterapeutico set regimenomeespecificado ='",
            regimenomeespecificado,
            "'",
            " , codigoregime = '",
            cod_regime,
            "', regimeesquemaidart = '",
            regime,
            "' , regimeesquema = '",
            regime,
            "' , active =TRUE ",
            " where regimeid = ",
            id,
            " ;"
          )
        )
        found <- TRUE
        break
        
      }
      
    }
    
    if (!found) {
      regimeid <- regimes_terap$regimeid[i]
      regimeid <-
        as.numeric(regimeid) + 32124 + sample(18:38, 1) * (i + v)
      cod_regime <- regimes_padronizados$codigoregime[i]
      regime_esquema <- regimes_padronizados$regimeesquema[i]
      regimenomeespecificado <-
        regimes_padronizados$regimenomeespecificado[i]
      regimeesquemaidart <- regimes_padronizados$regimeesquemaidart[i]
      
      insertRegimeTerapeutico(
        con_local,
        regimeid ,
        regime_esquema,
        cod_regime,
        active = TRUE,
        regimenomeespecificado,
        regimeesquemaidart)
      
    }
    
  }
  
}



## Apos padronizacao desactiva regimes sem uuid
dbExecute(con_local,sql_desactiva_regimes_sem_uuid)

# 
dbExecute(con_local,'update patient set uuidopenmrs =uuid')
# inseri generic provider
insertGenericProvider(con_local)



## 1 --  Primeiro executar o script com alteracoes atraves do pg admin
## 
# Deve-se mudar o nome de Clinic name , StockCenter name no iDART de modo a ser igual ao default_location  no openmrs
new_clinic_name  <- getOpenmrsDefaultLocation(con_openmrs,'openmrs')
idart_clinic_name <- getIdartClinicName(con_local)
facility_name <- getIdartFacilityName(con_local)
stockcenter_name <- getIdartStockCenterName(con_local)



## Update facility name , stockcenter name clinic name
status <- dbExecute(  con_local,  paste0("  update public.nationalclinics set facilityname = '",new_clinic_name,"' where facilityname = '",idart_clinic_name,"';"))
if(status==1){
  message('facilityname Actualizado com Sucesso')
}else {
  message('Nao foi possivel inserir o facilityname no iDART Insira manualmente')
  
}


status <- dbExecute(con_local,  paste0("update public.clinic    set clinicname = '",new_clinic_name,"' where clinicname = '",facility_name,"';"))
if(status==1){
  message('clinicname Actualizado com Sucesso')
}else {
  message('Nao foi possivel inserir o clinicname no iDART Insira manualmente')
  
}


status <-  dbExecute(  con_local,  paste0("update public.stockcenter    set stockcentername = '",new_clinic_name,"' where stockcentername = '",stockcenter_name,"';"))
if(status==1){
  message('stockcentername Actualizado com Sucesso')
}else {
  message('Nao foi possivel inserir o stockcentername no iDART Insira manualmente')
  
}