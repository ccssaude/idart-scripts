
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
postgres.db.name='pharm'
postgres.host='172.17.0.3'
postgres.port=5432
# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
############################################################################################################################################


regimsOpenMRS <- getRegimesOpenMRS(con_openmrs)
