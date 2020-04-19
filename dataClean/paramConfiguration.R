# Packages que contem algumas funcoes a serem usadas. Deve-se  garantir que tem todos os packages instalados.
# Para instalar deve: ligar o pc a net e  na consola digitar a instrucao -  ex: install.packages("plyr") depois install.packages("stringi ") assim sucessivamente
require(RMySQL)
require(plyr)    
require(stringi)
require(RPostgreSQL)
require(stringr)
require(tidyr)
require(stringdist)
require(dplyr)  
require(writexl)
####################################### Configuracao de Parametros  ##########################################################################
###############################################################################################################################################


wd <- '~/R/iDART/idart-scripts/dataClean/'      # ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

tipo_nid <- 'seq/ano'                           # ******** pode ser 'seq/ano' (formatNidMisau) ou 'ano/seq' (formatNidMisauV1)
# Modificar consoante o padrao de nids na US ( verificar a tabela dos duplicados para tomar decisao)

## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                          # ******** modificar
openmrs.password='esaude'                       # ******** modificar
openmrs.db.name='maio'                      # ******** modificar
openmrs.host='127.17.0.2'                       # ******** modificar
openmrs.port=3333                               # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)



# iDART  - Configuracao de variaveis de conexao 
postgres.user ='postgres'                      # ******** modificar
postgres.password='postgres'                   # ******** modificar
postgres.db.name='maio'                    # ******** modificar
postgres.host='127.17.0.3'                     # ******** modificar
postgres.port=5432                             # ******** modificar
# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)

####################################### Final da config  de Parametros  ########################################################################
################################################################################################################################################
#' Verifica se os ficheiros necessarios para executar as operacoes existem
#' 
#' @param files  nomes dos ficheiros
#'  @param dir  directorio onde ficam os files
#' @return TRUE/FALSE
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
checkScriptsExists <- function (files, dir){
  for(i in 1:length(files)){
    f <- files[i]
    if(!file.exists(paste0(dir,f))){
      message(paste0('Erro - Ficheiro ', f, ' nao existe em ',dir))
      return(FALSE)
    }
  }
  return(TRUE)
}
################################################################################################################################################


if (dir.exists(wd)){
 

  setwd(wd)    # set working directory - @ctiva o directorio wd
  if(checkScriptsExists(files = c('helper_functions_duplicated.R','genericFunctions.R','nidRelatedFunctions.R','logsExecucao.Rdata','Duplicated.R'),dir = wd)){
    
    
    
    
    source('helper_functions_duplicated.R')  ## Carregar funcoes
    source('genericFunctions.R')             ## Carregar funcoes
    source('nidRelatedFunctions.R')          ## Carregar funcoes
    load(file = 'logsExecucao.Rdata')        ## carrega a tabela dos logs

  } else{
    message( paste0('Ficheiros em falta. Veja o erro anterior'))
  }
  
}else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}
