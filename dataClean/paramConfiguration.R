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


## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                          # ******** modificar
openmrs.password='esaude'                       # ******** modificar
openmrs.db.name='canico'                      # ******** modificar
openmrs.host='127.17.0.2'                       # ******** modificar
openmrs.port=3333                               # ******** modificar

postgres.user ='postgres'                      # ******** modificar
postgres.password='postgres'                   # ******** modificar
postgres.db.name='canico'                    # ******** modificar
postgres.host='127.17.0.3'                     # ******** modificar
postgres.port=5432                             # ******** modificar

####################################### Final da config  de Parametros  ########################################################################
################################################################################################################################################





















































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
    
    
    status_con_openmrs <- tryCatch({
      
      message(paste0( "MySQL Conectando-se a: ", openmrs.host, '- db:',openmrs.db.name, "...") )
      
      # Objecto de connexao com a bd openmrs
      con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)

      ## Pacientes
      ## Buscar todos pacientes OpenMRS & iDART
      openmrsAllPatients <- getAllPatientsOpenMRS(con_openmrs)
      openmrsAllPatients$identifierSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   
      openmrsAllPatients$patientidSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   
      
      # Busca o codigo da US
      us.code= getOpenmrsUsCode(openmrsAllPatients)   
      
      us.name <- getOpenmrsDefaultLocation(con_openmrs) 
      
      
      
      tipo_nid = getTipoSeqNidUs(openmrsAllPatients)    # ******** pode ser 'seq/ano' (formatNidMisau) ou 'ano/seq' (formatNidMisauV1)
      # Modificar consoante o padrao de nids na US ( verificar a tabela dos duplicados para tomar decisao)
      
      
      
      1

      
    },
    error = function(cond) {
      
      message(paste0( "MySQL - Nao foi possivel connectar-se a host: ", openmrs.host, '  db:',openmrs.db.name, "...",'user:',openmrs.db.name, ' passwd: ', openmrs.password)) 
      message(cond)
      #Choose a return value in case of error
      return(0)
    },
    warning = function(cond) {
      message("Here's the original warning message:")
      message(cond)
      # Choose a return value in case of warning
      return(1)
    },
    finally = {
      # NOTE:
      # Here goes everything that should be executed at the end,
      # regardless of success or error.
      # If you want more than one expression to be executed, then you
      # need to wrap them in curly brackets ({...}); otherwise you could
      # just have written 'finally=<expression>'
      
    })
    
    
    status_con_idart<- tryCatch({
      
      message(paste0( "Postgres - conectando-se a host: ", postgres.host, ' - db:',postgres.db.name, "...") )
      
      # Objecto de connexao com a bd openmrs postgreSQL
      con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
      idartAllPatients <- getAllPatientsIdart(con_postgres)
      idartAllPatients$patientidSemLetras <- sapply(idartAllPatients$patientid, removeLettersFromNid)    
      
      
       1
      
      
    },
    error = function(cond) {
      
      message(paste0( "PosgreSQL - Nao foi possivel connectar-se a host: ", openmrs.host, '  db:',openmrs.db.name, "...",'user:',openmrs.db.name, ' passwd: ', openmrs.password)) 
      message(cond)
      #Choose a return value in case of error
      # Choose a return value in case of error
      return(0)
    },
    warning = function(cond) {
      message("Here's the original warning message:")
      message(cond)
      # Choose a return value in case of warning
      return(1)
    },
    finally = {
      # NOTE:
      # Here goes everything that should be executed at the end,
      # regardless of success or error.
      # If you want more than one expression to be executed, then you
      # need to wrap them in curly brackets ({...}); otherwise you could
      # just have written 'finally=<expression>'
      
    })
    
    
    if(status_con_openmrs==1){
      
      if(status_con_idart==1){
        
        message("Conexoes OpenMRS & iDART estabelecidas")  ## carrega as librarias
        
    
      } else {
        
        message("Algo correu mal, veja os erros na console")
        rm(openmrsAllPatients)
        rm(idartAllPatients)
        rm(con_postgres)
        rm(con_openmrs)
      }
    } else {
      
      message("Algo correu mal, veja os erros na console")
      rm(openmrsAllPatients)
      rm(idartAllPatients)
      rm(con_postgres)
      rm(con_openmrs)
    }
    
  
  } else{
    message( paste0('Ficheiros em falta. Veja o erro anterior'))
  }
  
}else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}
