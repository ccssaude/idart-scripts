# Packages que contem algumas funcoes a serem usadas 
require(RMySQL)
require(plyr)    ## Este package contem a funcao rbind.fill para fazer union all de 2 dataframes (tabelas)
require(stringi)
require(RPostgreSQL)
require(stringr)
require(tidyr)
require(stringdist)
require(dplyr)  ## Este package contem a funcao inner_join

####################################### Configuracao de Parametros  ###########################################################################
###############################################################################################################################################
wd <- '~/R/iDART/idart-scripts/dataClean/'  ## Configure para qualquer directorio , onde guardas os ficheiros helperFunctions.R  helper_fucntions_idart.R
                                  ## dataConsistency.R Duplicated.R
setwd(wd)
source('helper_functions_duplicated.R')  ## Carregar funcoes
source('nidRelatedFunctions.R')

## OpenMRS Stuff - Configuracoes de variaveis de conexao 
openmrs.user ='esaude'
openmrs.password='esaude'
openmrs.db.name='1maio'
openmrs.host='127.0.0.2'
openmrs.port=3333
us.code= '0111030701' #(Cod US 1Maio) 
# modificar este parametro para cada US. Este e o Cod da US definido pelo MISAU e geralmente e a primeira parte do NID
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
############################################################################################################################################


## Pacientes
## Buscar todos pacientes OpenMRS & iDART
openmrsAllPatients <- getAllPatientsOpenMRS(con_openmrs)
idartAllPatients <- getAllPatientsIdart(con_postgres)


## Pacientes Duplicados Por NID iDART & OpenMRS
duplicadosOpenmrs <- getDuplicatesPatOpenMRS(con_openmrs)
duplicadosiDART <-   getDuplicatesPatiDART(con_postgres)




# Cruzar duplicados iDART  com dados do openmrs
dups_idart_openmrs <- inner_join(duplicadosiDART,openmrsAllPatients, by=c("uuid"))
dups_idart_openmrs$solucao <- ""
#  Cria a tabela de logs
logsExecucao <- dups_idart_openmrs[1,c(1,2,4,8 )]
logsExecucao$accao <- ""
logsExecucao <- logsExecucao[0,c(1:5)]

#############################################  Para cada grupo de duplicados (NID) definir Solucao  #########################################################
#############################################  G 1,2,3,4 - Categorias de duplicados  ######################################################################### 
#############################################  CM - Corrigir Manualmente             #########################################################################
#############################################  CC - Corrigir atraves de codigo       #########################################################################
##############################################################################################################################################################  
############################################ Grupo 1 (G1): Pacientes  duplicados com mesmo uuid    ########################################################### 
                                                                                                                                                  ############ 
                                                                                                                                                  ############ 
## Grupo   (G1.1):  -Pacientes  duplicados no iDART e no OpenMRS, e  tem o mesmo uuid                                                             ############ 
## Solucao G1.1-CM                                                                                                                                ############ 
## Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados                           ############ 
                                                                                                                                                  ############ 
## Grupo   (G1.2):  -Pacientes  duplicados apenas iDART,  e tem o mesmo uuid                                                                      ############ 
## Solucao G1.2-CM                                                                                                                                ############
## Unir  os paciente no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados                                         ############ 
                                                                                                                                                  ############ 
                                                                                                                                                  ############ 
###########################################  Grupo 2 (G2): Pacientes  duplicados  com  uuids diferentes e nomes diferentes ###################################      
                                                                                                                                                  ############ 
## Grupo   2.1 (G2.1):  -Pacientes  duplicados  no iDART e no OpenMRS com  uuids diferentes e nomes diferentes sendo                              ############ 
##                      que os dois sao abandonos                                                                                                 ############ 
## Solucao G2.1:-CC                                                                                                                               ############
## Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: aquele que tiver a data do ult_lev menos recente               ########### 
## Verificar se o nid tem tamanho 21 (completo) e 2 barrar //                                                                                     ########### 
############ 
## Grupo   2.2 (G2.2):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao activos                            ############ 
## Solucao G2.2:   Trocar o nid de um dos pacientes, os dois estao activos, verificar processos clinicos                                          ############ 
                                                                                                                                                  ############ 
## Grupo   2.3 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo ############################# 
## Solucao (G2.3) - Trocar o nid do paciente que nao e activo                                                                                     ############ 
                                                                                                                                                  ############ 
## Grupo    2.4 (G2.4):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois pacientes ja nao sao activos          ############ 
## Solucao (G2.4) - Trocar o nid de  um dos pacientes                                                                                             ############ 
                                                                                                                                                  ############ 
## Grupo    2.5 (G2.5):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os um deles nao tem                           ############  
## estado TARV definido no OpenMRS############ 
## Solucao (G2.5) - Trocar o nid de  um dos pacientes############ 
############ 
## Grupo    2.6 (G2.6):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que o 2 n tem estado TARV ############ 
## definido no OpenMRS  e sao dups no OpenMRS
## Solucao (G2.6) - Trocar o nid de  um dos pacientes  no OpenMRS e o correspondente no iDART############ 
############ 
## Grupo   2.7 (G1.7):  -Pacientes com mesmo uuid iDART/OpenMRS mas os NIDs sao diferentes############ 
## Solucao G2.7-CC   Actualizar o NID no iDART , copiar do OpenMRS
############ 
############ 
################################  Grupo 3 (G3): Pacientes  duplicados  com uuids diferentes e nomes semelhantes  ( Algoritmo de simetria de Nomes)############ 
## Algoritimo  de simetria de strings com o method :  Jaro-Winker distance ver                                                                    ############
## https://cran.r-project.org/web/packages/stringdist/stringdist.pdf pag 19 & https://pt.wikipedia.org/wiki/Dist%C3%A2ncia_de_Jaro-Winkler        ############    
                                                                                                                                                  ############
## Grupo   3.1 (G3.1):  -Pacientes  duplicados no iDART e no OpenMRS , com nomes semelhantes                                                      ############        
## Solucao 3.1 (G3.1):  Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados      ############ 
                                                                                                                                                  ############ 
################################  Grupo 4 (G4): Pacientes  Triplicados############                                                                ############ 
                                                                                                                                                  ############ 
## Grupo   4.1 (G4.1):  - Dos 3 pacientes 2 tem dados iguais                                                                                      ############ 
## Solucao 4.1 (G4.1):  Unir  os paciente com os dados iguais                                                                                     ############ 
                                                                                                                                                  ############ 
##############################################################################################################################################################


## Solucao G2.7:   Actualizar o NID no iDART , copiar do OpenMRS
#dups_idart_openmrs <- updatePatSameUuuidDifNid(dups_idart_openmrs,con_postgres)



nidsAllDupsPatients <- unique(dups_idart_openmrs$patientid)

for (i in 1:length(nidsAllDupsPatients)) {
 
   nid_duplicado <- nidsAllDupsPatients[i]
  
   index <- which(dups_idart_openmrs$patientid==nid_duplicado)
   df_temp <- dups_idart_openmrs[index,]
  
  if(length(index)==2){
    
    if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){  ## Grupo 1 (G1)
      
      # Verificar se e duplicado no OpenMRS 
      if(dups_idart_openmrs$uuid[index[1]] %in% duplicadosOpenmrs$uuid){
        # Solucao Grupo 1 (G1.1)
        solucao <- "G1.1-CM -  Unir  os paciente no iDART e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados"
        dups_idart_openmrs$solucao[index[1]] <- solucao
        dups_idart_openmrs$solucao[index[2]] <- solucao
      } else
      {
        # nao e duplicado no OpenMRS
        # Solucao Grupo 1.2 (G1.2)
        solucao <- "G1.2-CM - Unir  os paciente no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados"
        dups_idart_openmrs$solucao[index[1]] <- solucao       
        dups_idart_openmrs$solucao[index[2]] <- solucao
      }
      
      
    } 
    
    else { 
      ## Grupo 2 (G2)
      #  avaliar o grau de simetria dos nomes , para saber se trata-se do mesmo paciente 
      
      nome_pat_1 <- dups_idart_openmrs$full_name[index[1]]
      nome_pat_2 <- dups_idart_openmrs$full_name[index[2]]
      
      uuid_pat_1 <- dups_idart_openmrs$uuid[index[1]]
      uuid_pat_2 <- dups_idart_openmrs$uuid[index[2]]
      id_pat_1 <- dups_idart_openmrs$id[index[1]]
      id_pat_2 <- dups_idart_openmrs$id[index[2]]
      estado_tarv_1 <- dups_idart_openmrs$estado_tarv[index[1]]
      estado_tarv_2 <- dups_idart_openmrs$estado_tarv[index[2]]
      
      if(stringdist(nome_pat_1,nome_pat_2, method = "jw") > 0.1){     # Grupo 2 (G2)
                                                                      #s e o rsultado de stringdist for:
                                                                      # 0 - perfect match, 0.1 - minimo aceitavel definido, 1 - no match at al
        if(! (is.na(estado_tarv_1) | is.na(estado_tarv_2)) ) {
          
        if(estado_tarv_1 == estado_tarv_2 & estado_tarv_2=='ABANDONO')        { 
          # Grupo 2.1 (G2.1) Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao abandonos 
          
          if(nid_duplicado %in% duplicadosOpenmrs$Nid){
          
          pac_lev_menos_rec <- getPacLevMenosRecente(index,nid_duplicado)[3]
          solucao <- paste0("G2.1 - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", pac_lev_menos_rec, " por ter a data do ult lev menos recente")
          dups_idart_openmrs$solucao[index[1]] <- solucao      
          dups_idart_openmrs$solucao[index[2]] <- solucao
          # Solucao G2.1:-CC  TODO
          if(checkNidFormat(nid_duplicado)){
            if(nid_duplicado %in% duplicadosOpenmrs$Nid){
              
              
              
            }
          } 
          }
        } 
        else  if(estado_tarv_1 != estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){ 
          
          if( 'TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){  # Solucao (G2.2) - Os dois pacientes estao activos em tarv 
            
            if(nid_duplicado %in% duplicadosOpenmrs$Nid){
              
            solucao <- paste0("G2.2 - Pacientes duplicados no iDART e OpenMRS, Trocar o nid de um dos pacientes,  verificar processos clinicos") 
            dups_idart_openmrs$solucao[index[1]] <- solucao     
            dups_idart_openmrs$solucao[index[2]] <- solucao 
            } else {
              
              solucao <- paste0("G2.2 - Trocar o nid de um dos pacientes,  verificar processos clinicos") 
              dups_idart_openmrs$solucao[index[1]] <- solucao     
              dups_idart_openmrs$solucao[index[2]] <- solucao 
            }
            
            
          } 
          else {
            if(estado_tarv_1== 'ACTIVO NO PROGRAMA'){  # Grupo 2.3 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
              if(nid_duplicado %in% duplicadosOpenmrs$Nid){
              idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
              nome_pat_nao_activo <- dups_idart_openmrs$full_name[idex]
              
              solucao <- paste0("G2.3 - Trocar no OpenMRS e iDART o  nid do paciente : ", nome_pat_nao_activo, " , ele nao e activo em tarv")
              dups_idart_openmrs$solucao[index[1]] <- solucao      
              dups_idart_openmrs$solucao[index[2]] <- solucao
              
              } 
              else {
                idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
                nome_pat_nao_activo <- dups_idart_openmrs$full_name[idex]
                
                solucao <- paste0("G2.3 - Trocar o nid do paciente : ", nome_pat_nao_activo, " ,  ele nao e activo em tarv")
                dups_idart_openmrs$solucao[index[1]] <- solucao      
                dups_idart_openmrs$solucao[index[2]] <- solucao
              }
            } 
            else {
               ## 
              if(nid_duplicado %in% duplicadosOpenmrs$Nid){
              idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_1 & dups_idart_openmrs$patientid==nid_duplicado)
              nome_pat_transf <- dups_idart_openmrs$full_name[idex]
              
              solucao <- paste0("G2.3 - Trocar o nid de um dos pacientes no OpenMRS e iDART de preferencia: ", nome_pat_transf, " , ele nao e activo em tarv")
              dups_idart_openmrs$solucao[index[1]] <- solucao      
              dups_idart_openmrs$solucao[index[2]] <- solucao
              } else {
                idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_1 & dups_idart_openmrs$patientid==nid_duplicado)
                nome_pat_transf <- dups_idart_openmrs$full_name[idex]
                
                solucao <- paste0("G2.3 - Trocar o nid de um dos pacientes de preferencia: ", nome_pat_transf, " , ele nao e activo em tarv")
                dups_idart_openmrs$solucao[index[1]] <- solucao      
                dups_idart_openmrs$solucao[index[2]] <- solucao
                
              }
              
              
            }
            
            
          
          
        }
          }
        else  if(estado_tarv_1 == estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){
          
          if(nid_duplicado %in% duplicadosOpenmrs$Nid){
            
            solucao <- paste0("G2.2 - Pacientes duplicados no iDART e OpenMRS, Trocar o nid de um dos pacientes,  verificar processos clinicos") 
            dups_idart_openmrs$solucao[index[1]] <- solucao     
            dups_idart_openmrs$solucao[index[2]] <- solucao 
          } else {
            
            solucao <- paste0("G2.2 - Trocar o nid de um dos pacientes,  verificar processos clinicos") 
            dups_idart_openmrs$solucao[index[1]] <- solucao     
            dups_idart_openmrs$solucao[index[2]] <- solucao 
          }
          
        }
           else  {
          
          if(  'TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){
               if(estado_tarv_1=='TRANSFERIDO DE'){
                 if(nid_duplicado %in% duplicadosOpenmrs$Nid){ 
                 idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
                 nome_pat_nao_activo <- dups_idart_openmrs$full_name[idex]
                 solucao <- paste0("G2.3 - Trocar o nid de um dos pacientes no OpenMRS e iDART de preferencia: ", nome_pat_nao_activo, " por ter saido do Tarv")
                 dups_idart_openmrs$solucao[index[1]] <- solucao 
                 dups_idart_openmrs$solucao[index[2]] <- solucao 
               } 
               else{
                 idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
                 nome_pat_nao_activo <- dups_idart_openmrs$full_name[idex]
                 solucao <- paste0("G2.3 - Trocar o nid de um dos pacientes no iDART de preferencia: ", nome_pat_nao_activo, " por ter saido do Tarv")
                 dups_idart_openmrs$solucao[index[1]] <- solucao 
                 dups_idart_openmrs$solucao[index[2]] <- solucao 
                 
               }
            
          }}
          else { ## Ambos pacientes ja nao fazem tarv, trocar o nid de qualquer um dos 2
            
            # Solucao (G2.4) - Os dois pacientes nao estao activos em tarv 
            if(nid_duplicado %in% duplicadosOpenmrs$Nid){
              solucao <- paste0("G2.4 - Trocar o nid de um dos pacientes no OpenMRS e  o Seu correspondente no iDART, os dois nao estao activos") 
              dups_idart_openmrs$solucao[index[1]] <- solucao 
              dups_idart_openmrs$solucao[index[2]] <- solucao 
            } else {
              
              solucao <- paste0("G2.4 - Trocar o nid de um dos pacientes no iDART, os dois nao estao activos") 
              dups_idart_openmrs$solucao[index[1]] <- solucao 
              dups_idart_openmrs$solucao[index[2]] <- solucao 
              
            }
            
            
          }

          }
        
        }
        else { 
            
          if( (is.na(estado_tarv_1) | is.na(estado_tarv_2)) ) {
            
            if(is.na(estado_tarv_1)){
              
              if(estado_tarv_2 %in% c('ABANDONO','TRANSFERIDO PARA','OBITO')){
                # Verificar se e duplicado no OpenMRS  haha
                if(nid_duplicado %in% duplicadosOpenmrs$Nid){
                  idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_2 & dups_idart_openmrs$patientid==nid_duplicado)
                  nome_pat_saiu<- dups_idart_openmrs$full_name[idex]
                  
                  solucao <- paste0("G2.5-  Trocar no OpenMRS e o correspondente no iDART o nid do Paciente: ",nome_pat_saiu," Pois, ja nao esta a fazer o tratamento")
                  dups_idart_openmrs$solucao[index[1]] <- solucao
                  dups_idart_openmrs$solucao[index[2]] <- solucao
                } 
                else {
                  
                  
                  solucao <- paste0("G2.5-  Trocar no   iDART o nid do Paciente: ",nome_pat_saiu," Pois, ja nao esta a fazer o tratamento")
                  dups_idart_openmrs$solucao[index[1]] <- solucao
                  dups_idart_openmrs$solucao[index[2]] <- solucao
                }
              
                
              }
              else { # TODO codigo de 2 pacientes sem estados
                } 
              }             
            else {
                
                if(estado_tarv_1 %in% c('ABANDONO','TRANSFERIDO PARA','OBITO')){
                  # Verificar se e duplicado no OpenMRS  haha
                  if(nid_duplicado %in% duplicadosOpenmrs$Nid){
                    idex <-which(dups_idart_openmrs$estado_tarv==estado_tarv_1 & dups_idart_openmrs$patientid==nid_duplicado)
                    nome_pat_saiu<- dups_idart_openmrs$full_name[idex]
                    
                    solucao <- paste0("G2.5-  Trocar no OpenMRS e o correspondente no iDART o nid do Paciente: ",nome_pat_saiu," Pois, ja nao esta a fazer o tratamento")
                    dups_idart_openmrs$solucao[index[1]] <- solucao
                    dups_idart_openmrs$solucao[index[2]] <- solucao
                  } else {
                    
                    
                    solucao <- paste0("G2.5-  Trocar no   iDART o nid do Paciente: ",nome_pat_saiu," Pois, ja nao esta a fazer o tratamento")
                    dups_idart_openmrs$solucao[index[1]] <- solucao
                    dups_idart_openmrs$solucao[index[2]] <- solucao
                  }
                
                
              }
                else {
                # estad_tarv_1 e transferido de 
                # Verificar se e duplicado no OpenMRS  
                if(nid_duplicado %in% duplicadosOpenmrs$Nid){
                  # Solucao (G2.5) - Um dos  pacientes nao tem estado no programa TARV  OpenMRS
                  solucao <- "G2.5-  Trocar o nid de  um dos pacientes  no OpenMRS e o correspondente no iDART"
                  dups_idart_openmrs$solucao[index[1]] <- solucao
                  dups_idart_openmrs$solucao[index[2]] <- solucao
                } else
                  # Solucao (G2.5) - Um dos  pacientes nao tem estado no programa TARV  OpenMRS
                  solucao <- paste0("G2.5 - Trocar o nid de um dos pacientes no iDART") 
                  dups_idart_openmrs$solucao[index[1]] <- solucao   
                 dups_idart_openmrs$solucao[index[2]] <- solucao
              }
                
                
              }
            }

          }
        
        }
      else {  #  Nomes sao semelhantes , podemos assumir que trata-se do mesmo pacientes
        
          # Verficar se sao duplicados no openmrs 
          # Grupo   3.1 (G3.1):  -Pacientes  duplicados no iDART e no OpenMRS , com nomes semelhantes
          # priorizar o que tiver levantamentos mais actualizados
          # if(nid_duplicado %in% duplicadosOpenmrs$Nid){
          # Solucao Grupo 3.1 (G3.1)
          nome_lev_mais_act <- getPacLevMaisRecente(index,nid_duplicado)
          solucao <- paste0("G3.1 - Unir  os paciente ",nome_pat_1," e ",nome_pat_2, " no iDART e no OpenMRS. Paciente com nome: ",
                            nome_lev_mais_act, " e preferido por ter a data do ult lev mais recente")
          dups_idart_openmrs$solucao[index[1]] <- solucao  
          dups_idart_openmrs$solucao[index[2]] <- solucao
          
    
        
      }
    
  }
  }
  
  else if(length(index)==3){   # Grupo G4
    
    if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){ 

      # Verificar se e duplicado no OpenMRS 
      if(dups_idart_openmrs$uuid[index[1]] %in% duplicadosOpenmrs$uuid){
    
        nome_pat <- dups_idart_openmrs$full_name[index[1]]
        solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados no iDART")
        dups_idart_openmrs$solucao[index[1]] <- solucao
        dups_idart_openmrs$solucao[index[2]] <- solucao
      }
      else  {
        # nao e duplicado no OpenMRS
        nome_pat <- dups_idart_openmrs$full_name[index[1]]
        solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados")
        dups_idart_openmrs$solucao[index[1]] <- solucao
        dups_idart_openmrs$solucao[index[2]] <- solucao
      }
      
      
    } 
    if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[3]] ){ 
        
        # Verificar se e duplicado no OpenMRS 
        if(dups_idart_openmrs$uuid[index[1]] %in% duplicadosOpenmrs$uuid){
          nome_pat <- dups_idart_openmrs$full_name[index[1]]
          solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados no iDART")
          dups_idart_openmrs$solucao[index[1]] <- solucao
          dups_idart_openmrs$solucao[index[3]] <- solucao
        }   else {
          # nao e duplicado no OpenMRS
          nome_pat <- dups_idart_openmrs$full_name[index[1]]
          solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados")
          dups_idart_openmrs$solucao[index[1]] <- solucao
          dups_idart_openmrs$solucao[index[3]] <- solucao
        }
        
        
    }  
    if(dups_idart_openmrs$uuid[index[2]] == dups_idart_openmrs$uuid[index[3]] ){ 
      
      # Verificar se e duplicado no OpenMRS 
      if(dups_idart_openmrs$uuid[index[2]] %in% duplicadosOpenmrs$uuid){
        nome_pat <- dups_idart_openmrs$full_name[index[2]]
        solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados no iDART")
        dups_idart_openmrs$solucao[index[2]] <- solucao
        dups_idart_openmrs$solucao[index[3]] <- solucao
      } 
      else {
        # nao e duplicado no OpenMRS
        nome_pat <- dups_idart_openmrs$full_name[index[2]]
        solucao <- paste0("G4.1 -  Unir os  2 Pacientes  os paciente com nome: ",nome_pat," no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados")
        dups_idart_openmrs$solucao[index[1]] <- solucao
        dups_idart_openmrs$solucao[index[3]] <- solucao
      }
      
      
    }
      
  }
    
 # else{}  caso de pacientes quadriplicados
  
  }
    
    
  



a = subset(dups_idart_openmrs, TRUE,c('uuid',  'patientid',
                                        'full_name',
                                       'dateofbirth',
                                       'identifier',
                                       'full_name_openmrs',
                                       'birthdate',
                                       'data_ult_levant',
                                       'estado_tarv' , 'solucao'
))
