#############################################  Para cada grupo de duplicados (NID) definir Solucao  #########################################################
#############################################  G 1,2,3,4 - Categorias de duplicados  ######################################################################### 
#############################################  CM - Corrigir Manualmente             #########################################################################
#############################################  CC - Corrigir Automaticamente       #########################################################################
##############################################################################################################################################################  
############################################ Grupo 1 (G1): Pacientes  duplicados com mesmo uuid    ########################################################### 
############ 
## Grupo   (G1.1):  -Pacientes  duplicados no iDART e no OpenMRS, e  tem o mesmo uuid                                                             ############ 
## Solucao G1.1-CM                                                                                                                                ############ 
## Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados                           ############ 
##############################################################################################################################################################                                                                                                                                                 ############ 
## Grupo      (G1.2): Pacientes  duplicados apenas iDART,  e tem o mesmo uuid                                                                     ############ 
## Solucao G1.2-CM    Unir  os paciente no iDART , o   paciente preferido e aquele que tiver levantamentos mais actualizados                      ############                                                                                                         ############
##                              
##############################################################################################################################################################
###########################################  Grupo 2 (G2): Pacientes  duplicados  com  uuids diferentes e nomes diferentes ###################################      
############ 
## Grupo   2.1 (G2.1):-CC Pacientes  duplicados  no iDART e no OpenMRS com  uuids diferentes e nomes diferentes sendo                                ############ 
##                      que os dois sao abandonos                                                                                                 ############ 
## Solucao G2.1:-CC                                                                                                                               ############
## Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: aquele que tiver a data do ult_lev menos recente               ############ 
##############################################################################################################################################################
## Grupo   2.2 (G2.2):-CC Pacientes  duplicados  iDART e no OpenMRS com  uuids diferentes e nomes diferentes sendo que os dois sao activos           ############ 
## Solucao G2.2:-CC    Trocar o nid de um dos pacientes, os dois estao activos, de preferencia aquele que tiver a data do ult_lev menos recente   ############ 
##                                                                                                                                                ############ 
##############################################################################################################################################################  
## Grupo   2.2.1 (G2.2.1):  Pacientes  duplicados  apenas no iDART  com  uuids diferentes e nomes diferentes                                      ############ 
## Solucao G2.2.1:-CC       Provavelmete um paciente nao existe no OpenMRS, ou e transito, verificar se o uuid deste paciente existe na BD OpenMRS############
##                          se exisitir buscar comparar os NIDs e actualizar . Se um for Transito modificar o nid do paciente transito            ############ 
############################################################################################################################################################## 
## Grupo   2.3 (G2.3):  -Pacientes  duplicados  no iDART e OpenMRS com  uuids diferentes e nomes diferentes sendo que um dos
##                       pacientes nao e activo                                                                                                   ############ 
## Solucao (G2.3):-CC    Trocar o nid do paciente que nao e activo                                                                                ############ 
############################################################################################################################################################## 
############ 
## Grupo    2.4 (G2.4):  Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois pacientes ja nao sao activos           ############ 
## Solucao  G2.4:CC -     Trocar o nid de  um dos pacientes  de preferencia o que tiver a data de lev menos recente                               ############ 
#############################################################################################################################################################
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
## Solucao 3.1:-CM (G3.1):  Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados      ############ 
############ 
## Grupo   3.1.2 (G3.1.2):  -Pacientes  duplicados no iDART sem informacao no openmrs , existem no iDART com outros NIDs                         ############        
## Solucao 3.1.2:-CM (G3.1.2):  Unir  os paciente no iDART                                                                                  ############ 
############ 
################################  Grupo 4 (G4): Pacientes  Triplicados############                                                                ############ 
############ 
## Grupo   4.1 (G4.1):  - Dos 3 pacientes 2 tem dados iguais                                                                                      ############ 
## Solucao 4.1 G4.1:-CM  Unir  os paciente com os dados iguais                                                                                     ############ 
############ 
##############################################################################################################################################################
##############################################################################################################################################################


# ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

wd <- '~/R/iDART/idart-scripts/dataClean/'

# Limpar o envinronment

rm(list=setdiff(ls(), c("wd", "tipo_nid") ))

if (dir.exists(wd)){
  
    setwd(wd) 
    source('paramConfiguration.R')                     ##  Carrega dados actualizados
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}


##############################################################################################################################################################
##############################################################################################################################################################      
      
      ## Pacientes Duplicados Por NID iDART & OpenMRS
      duplicadosOpenmrs <- getDuplicatesPatOpenMRS(con_openmrs)
      temp <- getPatientsInvestigar(con_openmrs)
      
      duplicadosiDART <-   getDuplicatesPatiDART(con_postgres)
      ## Pacientes da FARMAC ( vamos garantir que nao modificamos nids destes pacientes)
      farmac_pacients  <- dbGetQuery(con_postgres,paste0("select distinct patientid, patientfirstname , patientlastname
                                                  from (select distinct patientid, patientfirstname , patientlastname from
                                                  sync_temp_dispense sd  where sync_temp_dispenseid='", us.name, "' union all 
                                                  select distinct patientid, firstnames as  patientfirstname , lastname as patientlastname from
                                                  sync_temp_patients sp ) all_p order by all_p.patientid"))

      
      # Cruzar duplicados iDART  com dados do openmrs
      dups_idart_openmrs <- inner_join(duplicadosiDART,openmrsAllPatients, by=c("uuid"))
      dups_idart_openmrs$solucao <- ""

  
##############################################################################################################################################################
#########################      Iniciar o processamento                 ######################################################################
      ##  Nids duplicados e guarda num vector
      nidsAllDupsPatients <- unique(dups_idart_openmrs$patientid)
      
      ##  Remover pacientes da  farmac do vector dos pacientes dos duplicados (evitar problemas com FARMAC)
      if(is.data.frame(farmac_pacients) ){
        nidsAllDupsPatients <- nidsAllDupsPatients [! nidsAllDupsPatients %in% farmac_pacients$patientid]
        pat_farmac_dups <- duplicadosiDART [ duplicadosiDART$patientid %in% farmac_pacients$patientid,]
        if(dim(pat_farmac_dups)[1]> 0){
          
          write_xlsx(x = pat_farmac_dups,
                     path = paste0('output/',us.name, ' - Pacientes Farmac Duplicados.xlsx'),
                     col_names = TRUE,
                     format_headers = TRUE
          )
        }

      } 

      for (i in 1:length(nidsAllDupsPatients)) {
        
        nid_duplicado <- nidsAllDupsPatients[i]
       
         index <- which(dups_idart_openmrs$patientid==nid_duplicado)
        
        
        if(length(index)==2){
          
          if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]]  ){  ## Grupo 1 (G1)
            
            if( dups_idart_openmrs$uuid[index[1]]  %in% duplicadosOpenmrs$uuid){
              
              # Solucao Grupo 1 (G1.1)
              solucao <- paste0("G1.1-CM  Unir  os paciente no iDART & OpenMRS com o NID: (", dups_idart_openmrs$patientid[index[1]] ," - " ,
                                dups_idart_openmrs$full_name[index[1]],
                                ") com  (" ,dups_idart_openmrs$patientid[index[2]] ,
                                " - " ,dups_idart_openmrs$full_name[index[2]],
                                ") no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
              logSolucao(index[1],index[2] ,solucao)  
              logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            } else {
              
              # Solucao Grupo 1 (G1.1)
              solucao <- paste0("G1.1-CM  Unir  os paciente no iDART com o NID: (", dups_idart_openmrs$patientid[index[1]] ," - " ,
                                dups_idart_openmrs$full_name[index[1]],
                                ") com  (" ,dups_idart_openmrs$patientid[index[2]] ,
                                " - " ,dups_idart_openmrs$full_name[index[2]],
                                ") no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
              logSolucao(index[1],index[2] ,solucao)  
              logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            }
          
          } 
          
          else { 
            
            nome_pat_1 <- dups_idart_openmrs$full_name[index[1]]
            nome_pat_2 <- dups_idart_openmrs$full_name[index[2]]
            
            estado_tarv_1 <- dups_idart_openmrs$estado_tarv[index[1]]
            estado_tarv_2 <- dups_idart_openmrs$estado_tarv[index[2]]
            #################################    Grupo 2 (G2)   ###########################################
            #  Avaliar o grau de simetria dos nomes , para saber se trata-se do mesmo paciente 
            #Algoritimo  de simetria de strings com o method :  Jaro-Winker distance (eficiente para comparacao de nomes)
            if(stringdist(nome_pat_1,nome_pat_2, method = "jw") > 0.14){     # Grupo 2 (G2)
              #s e o rsultado de stringdist for:
              if(! (is.na(estado_tarv_1) | is.na(estado_tarv_2)) ) {
                
                # Grupo 2.1 (G2.1) Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao abandonos 
                # Solucao G2.1:-CC  
                if(estado_tarv_1 == estado_tarv_2 & estado_tarv_2=='ABANDONO'){ 
                  
                  patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado, df =dups_idart_openmrs)
                  new_nid = 0
                  
                  if(tipo_nid=='Seq/Ano'){
                    new_nid <- getNewNid(patient_to_update[3])  
                  }else if(tipo_nid=='Ano/Seq')
                  { new_nid <- getNewNidV1(patient_to_update[3])    }
                  else {
                    message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                     break
                    }

                  if( 0 != new_nid){
                    solucao <- paste0("G2.1:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ",nid_duplicado ,' - ',  patient_to_update[5], " por ser abandono e  ter a data do ult lev menos recente")
                    logSolucao(index[1],index[2] ,solucao)  
                    beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                  } else {  
                    solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                    logSolucao(index[1],index[2],solucao)
                    logAction(patient.info =patient_to_update,action = solucao )
                  }
                  
                  
                }  
                else if(estado_tarv_1 != estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){ 
                  
                  ## Grupo   2.2 (G2.2):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao activos                      
                  ## Solucao G2.2:-CC      Trocar o nid de um dos pacientes, os dois estao activos                   
                  # TODO - modificar o nid trasnferido
                  if( 'TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){  
                    
                    patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado, df =dups_idart_openmrs)
                    new_nid = 0
                    
                    if(tipo_nid=='Seq/Ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='Ano/Seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                      break
                    }
                    if( 0 != new_nid){
                      solucao <- paste0("G2.2:CC - Pacientes duplicados no iDART e OpenMRS, os 2 sao activos, Trocar o nid  :",nid_duplicado ,' - ',  patient_to_update[5],  ", por ter  a data do ult_lev menos recente") 
                      logSolucao(index[1],index[2],solucao)
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres) # kaka
                    } else{
                      solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                      logSolucao(index[1],index[2],solucao)
                      logAction(patient.info =patient_to_update,action = solucao )
                    }
                  }
                  else {
                    # Grupo 2.3 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
                    patient_to_update <- getPacInactivo(index=index,nid = nid_duplicado,df=dups_idart_openmrs)
                    new_nid = 0
                    
                    if(tipo_nid=='Seq/Ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='Ano/Seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                      break
                    }
                    if( 0 != new_nid){
                      solucao <- paste0("G2.3:-CC - Trocar no OpenMRS e iDART o  NID do paciente:",nid_duplicado ,' - ', patient_to_update[5],  "  ele nao e activo em tarv nesta US")
                      logSolucao(index[1],index[2],solucao)
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                    } else{
                      solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                      logSolucao(index[1],index[2],solucao)
                      logAction(patient.info =patient_to_update,action = solucao )
                      
                    }
                  }
                }
                else  if(estado_tarv_1 == estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){
                  #Solucao G2.2:-CC    Trocar o nid de um dos pacientes, os dois estao activos, de preferencia aquele que tiver a data do ult_lev menos recente
                  patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado, df =dups_idart_openmrs)
                  new_nid = 0
                  
                  if(tipo_nid=='Seq/Ano'){
                    new_nid <- getNewNid(patient_to_update[3])  
                  }else if(tipo_nid=='Ano/Seq')
                  { new_nid <- getNewNidV1(patient_to_update[3])    }
                  else {
                    message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                    break
                  }  
                  if( 0 != new_nid){
                    solucao <- paste0("G2.2:-CC  - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ",nid_duplicado ,' - ', 
                                      patient_to_update[5], " por ter a data do ult lev menos recente")
                    logSolucao(index[1],index[2] ,solucao)  
                    beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                  } else {
                    solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                    logSolucao(index[1],index[2],solucao)
                    logAction(patient.info =patient_to_update,action = solucao )
                  }
                }
                else {
                  
                  if('TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){
                    if(estado_tarv_1==estado_tarv_2){ ## os dois pacientes sao activos
                      
                      patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado, df =dups_idart_openmrs)
                      new_nid = 0
                      
                      if(tipo_nid=='Seq/Ano'){
                        new_nid <- getNewNid(patient_to_update[3])  
                      }else if(tipo_nid=='Ano/Seq')
                      { new_nid <- getNewNidV1(patient_to_update[3])    }
                      else {
                        message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                        break
                      }
                      if( 0 != new_nid){
                        solucao <- paste0("G2.3:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", nid_duplicado ,' - ', 
                                          patient_to_update[5], " por ter a data do ult lev menos recente")
                        logSolucao(index[1],index[2] ,solucao)  
                        beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                      } else{
                        
                        solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                        logSolucao(index[1],index[2],solucao)
                        logAction(patient.info =patient_to_update,action = solucao )
                      }
                    }
                    else{ 
                      
                      patient_to_update <- getPacInactivo(index=index,nid = nid_duplicado,df = dups_idart_openmrs)
                      new_nid = 0
                      
                      if(tipo_nid=='Seq/Ano'){
                        new_nid <- getNewNid(patient_to_update[3])  
                      }else if(tipo_nid=='Ano/Seq')
                      { new_nid <- getNewNidV1(patient_to_update[3])    }
                      else {
                        message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                        break
                      }  
                      if( 0 != new_nid){
                        solucao <- paste0("G2.3:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", nid_duplicado ,' - ', patient_to_update[5], " por ter saido do Tarv")
                        logSolucao(index[1],index[2] ,solucao)  
                        beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                      } else{
                        
                        solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                        logSolucao(index[1],index[2],solucao)
                        logAction(patient.info =patient_to_update,action = solucao )
                      }
                    }} 
                  else{
                    
                    ## Solucao  G2.4:CC -     Trocar o nid de  um dos pacientes  de preferencia o que tiver a data de lev menos recente    
                    patient_to_update <- getPacLevMaisRecente(index, nid = nid_duplicado, df=dups_idart_openmrs)
                    new_nid = 0
                    
                    if(tipo_nid=='Seq/Ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='Ano/Seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                      break
                    } 
                    if( 0 != new_nid){
                      solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por nao ser activo e ter a data do ult lev menos recente")
                      logSolucao(index[1],index[2] ,solucao)  
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                    }else{
                      solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                    logSolucao(index[1],index[2],solucao)
                    logAction(patient.info =patient_to_update,action = solucao )
                    }
                  }
                  
                }
                
              }
              else if (is.na(estado_tarv_1) & is.na(estado_tarv_2)) {
                
                # Solucao (G2.3):-CC Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS
                patient_to_update <- composePatientToUpdate(index[1],df = dups_idart_openmrs)
                new_nid = 0
                if(tipo_nid=='Seq/Ano'){
                  new_nid <- getNewNid(patient_to_update[3])  
                }else if(tipo_nid=='Ano/Seq')
                { new_nid <- getNewNidV1(patient_to_update[3])    }
                else {
                  message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                  break
                }  
                
                if( 0 != new_nid){
                  solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por ser duplicado e  nao  nao ter o estado de permanecia definido")
                  logSolucao(index[1],index[2] ,solucao)  
                  beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                }else{
                  
                  solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                  logSolucao(index[1],index[2],solucao)
                  logAction(patient.info =patient_to_update,action = solucao )
                }
              }
              else if(is.na(estado_tarv_1) | is.na(estado_tarv_2))  {
                
                index_pac_sem_estado_def <- which(is.na(c(estado_tarv_1,estado_tarv_2)))
                
                # Solucao (G2.3):-CC Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS
                patient_to_update <- composePatientToUpdate(index[index_pac_sem_estado_def],df = dups_idart_openmrs)
                new_nid = 0
                if(tipo_nid=='Seq/Ano'){
                  new_nid <- getNewNid(patient_to_update[3])  
                }else if(tipo_nid=='Ano/Seq')
                { new_nid <- getNewNidV1(patient_to_update[3])    }
                else {
                  message(" Nao foi possivel obter o tipo de Sequencia automaticamente  
                              verifica o tipo de Sequencia e defina manualmete a variavel tipo_nid = ('Ano/Seq'   ou 'Seq/Ano') geralmente para estes casos  costuma ser tipo_nid = 'Ano/Seq' )")
                  break
                }  
                
                if( 0 != new_nid){
                  solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por ser duplicado e  nao  nao ter o estado de permanecia definido")
                  logSolucao(index[1],index[2] ,solucao)  
                  beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                }else{
                  
                  
                  solucao <- paste0("NP_NID -  Nao foi possivel trocar o nid do paciente duplicado :",nid_duplicado ,' - ',  patient_to_update[5],  ", deve formatar manualemnte este nid") 
                  logSolucao(index[1],index[2],solucao)
                  logAction(patient.info =patient_to_update,action = solucao )
                }

              }
              else{
                #TODO nhuhm dos estados anteriores
                
              }
              
            }
            else { 
              #  Nomes sao semelhantes , podemos assumir que trata-se do mesmo pacientes
              
              ## Verficar se sao duplicados no openmrs 
              ## Grupo   3.1 (G3.1):  -Pacientes  duplicados no iDART e no OpenMRS , com nomes semelhantes
              ## Solucao G3.1:-CM (G3.1):  Unir  os paciente no iDART , e no OpenMRS, o   paciente preferido e aquele que tiver levantamentos mais actualizados
              #TODO verificar o sexo e idade dos dois paciente, se forem diferente 
              # Trocar o nid de um dos pacientes
              
              patient_to_update <-   getPacLevMaisRecente(index, nid = nid_duplicado, df=dups_idart_openmrs)
             
               solucao <-  paste0("G3.1:-CM  -  Unir  os paciente com o NID: (", nid_duplicado,' - ' , dups_idart_openmrs$full_name[index[1]] ,
                                 ") , com ",  "(", nid_duplicado,' - ' , dups_idart_openmrs$full_name[index[2]] ,") ,no iDART e no OpenMRS, o preferido e aquele que tiver maior numero de levantamentos ou, o paciente que nao esta no estado : ABANDONO/TRANSFERIDO/PARA/OBITO" )
              
              logSolucao(index[1], index[2] , solucao)
              logAction(composePatientToUpdate(index[1], df = dups_idart_openmrs),
                        solucao)
            
              
            }
            
          }
        }
        
        else if(length(index)==3){   # Grupo G4
          
          if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){
            
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[1]],' - ' ,
                              dups_idart_openmrs$full_name[index[1]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[2]],' - ' ,
                              dups_idart_openmrs$full_name[index[2]] , 
                              ") ,no iDART e no OpenMRS",
                              ' e , trocar o codigo de servico (01-> 02 ou 02 -> 01) do paciente: ',
                              dups_idart_openmrs$patientid[index[3]],' - ' ,
                              dups_idart_openmrs$full_name[index[3]]  )
            logSolucao(index[1],index[2] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            
            
          } 
          else if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[3]] ){
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[1]],' - ' ,
                              dups_idart_openmrs$full_name[index[1]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[3]],' - ' ,
                              dups_idart_openmrs$full_name[index[3]] , 
                              ") ,no iDART e no OpenMRS",
                              ' e , trocar o codigo de servico (01-> 02 ou 02 -> 01) do paciente: ',
                              dups_idart_openmrs$patientid[index[2]],' - ' ,
                              dups_idart_openmrs$full_name[index[2]]  )
            
              logSolucao(index[1],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            
            
          }
          else  if(dups_idart_openmrs$uuid[index[2]] == dups_idart_openmrs$uuid[index[3]] ){
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[2]],' - ' ,
                              dups_idart_openmrs$full_name[index[2]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[3]],' - ' ,
                              dups_idart_openmrs$full_name[index[3]] , 
                              ") ,no iDART e no OpenMRS",
                              ' e , trocar o codigo de servico (01-> 02 ou 02 -> 01) do paciente: ',
                              dups_idart_openmrs$patientid[index[1]],' - ' ,
                              dups_idart_openmrs$full_name[index[1]]  )
            
               logSolucao(index[2],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[2],df = dups_idart_openmrs),solucao  )
          }
          else {
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-PT nao foi possivel encontrar a solucao para os pacientes triplicados Nids:"
                              ,dups_idart_openmrs$patientid[index[1]], '- ' , dups_idart_openmrs$full_name[index[1]],
                              ', ',dups_idart_openmrs$patientid[index[2]], '- ' , dups_idart_openmrs$full_name[index[2]],
                              ' ,',dups_idart_openmrs$patientid[index[3]], '- ' , dups_idart_openmrs$full_name[index[3]],
                              "Deve-se corrigir manualmente no iDART & OpenMRS")
            
            logSolucao(index[1],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
          }
        }
        
        
      }
      
##############################################################################################################################################################
##############################################################################################################################################################

  ##  Exportar os Logs
  if (dim(logsExecucao)[1]>0){
        
        #corrigir_manualmente <- logsExecucao[which(grepl(pattern = 'CM',x=logsExecucao$accao,ignore.case = TRUE)),]
        pacientes_triplicados_sem_solucao_automa <- logsExecucao[which(grepl(pattern = 'G4.1-PT',x=logsExecucao$accao,ignore.case = TRUE)),]
        pacintes_triplicados_unir <-  logsExecucao[which(grepl(pattern = 'G4.1-CM',x=logsExecucao$accao,ignore.case = TRUE)),]
        pacintes_unir <-  logsExecucao[which(grepl(pattern =  '3.1:-CM',x=logsExecucao$accao,ignore.case = TRUE)),] 
        pacintes_unir_2 <-  logsExecucao[which(grepl(pattern = 'G1.1-CM',x=logsExecucao$accao,ignore.case = TRUE)),] 
        pacintes_nids_formatar_manualmente<-  logsExecucao[which(grepl(pattern = 'NP_NID',x=logsExecucao$accao,ignore.case = TRUE)),] 
        pacintes_erro_sql <-  logsExecucao[which(grepl(pattern = 'BD_ERROR',x=logsExecucao$accao,ignore.case = TRUE)),] 
        pacintes_erro_ss<-  logsExecucao[which(grepl(pattern = 'SS-CM',x=logsExecucao$accao,ignore.case = TRUE)),] 
  
      

        if(dim(pacintes_erro_ss)[1]>0){
          
          write_xlsx(
            pacintes_erro_ss,
            path = paste0('output/',us.name,' - Pacientes_duplicados_apenas_idart_que_nao_existem_openmrs.xlsx'),
            col_names = TRUE,
            format_headers = TRUE
          )
          logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_erro_ss$uuid)),]
        }
        if(dim(pacintes_unir_2)[1]>0){
          
          if(dim(pacintes_unir)[1]>0){
            
            temp <- rbind.fill(pacintes_unir,pacintes_unir_2)
            write_xlsx(
              temp,
              path = paste0('output/',us.name,' - Pacientes_para_unir_manualmente_no_openmrs_idart.xlsx'),
              col_names = TRUE,
              format_headers = TRUE
            )
            logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_unir$uuid)),]
            logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_unir_2$uuid)),]
          } else {
            write_xlsx(
              pacintes_unir_2,
              path = paste0('output/',us.name,' - Pacientes_para_unir_manualmente_no_openmrs_idart.xlsx'),
              col_names = TRUE,
              format_headers = TRUE
              )
            logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_unir_2$uuid)),]
        } }
        if(dim(pacintes_unir)[1]>0){
          
          write_xlsx(
            pacintes_unir,
            path = paste0('output/',us.name,' - Pacientes_para_unir_manualmente_no_openmrs_idart.xlsx'),
            col_names = TRUE,
            format_headers = TRUE
          )
          logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_unir$uuid)),]
        }
        if(dim(pacintes_triplicados_unir)[1]>0){

            write_xlsx(
              pacintes_triplicados_unir,
              path = paste0('output/',us.name,' - Pacientes_triplicados_para_unir_manualmente_openmrs_idart.xlsx'),
              col_names = TRUE,
              format_headers = TRUE
            )
          logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_triplicados_unir$uuid)),]
          }
        if(dim(pacientes_triplicados_sem_solucao_automa)[1]>0){
          
          write_xlsx(
            pacientes_triplicados_sem_solucao_automa,
            path = paste0('output/',us.name,' - Pacientes_triplicados_para_corrigir_manualmente_openmrs_idart.xlsx'),
            col_names = TRUE,
            format_headers = TRUE
          )
          logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacientes_triplicados_sem_solucao_automa$uuid)),]
          
          
        }
        if(dim(pacintes_nids_formatar_manualmente)[1]>0){
          
            write_xlsx(
              pacintes_nids_formatar_manualmente,
              path = paste0('output/',us.name,' - Pacientes_para_formatar_nids_manualmente_no_openmrs_idart.xlsx'),
              col_names = TRUE,
              format_headers = TRUE
            )
            logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_nids_formatar_manualmente$uuid)),]
          } 
        if(dim(pacintes_erro_sql)[1]>0){
          
          write_xlsx(
            pacintes_erro_sql,
            path = paste0('output/',us.name,' - Pacientes_que_tiveram_erro_sql_durante_a_correcao_proceder_manualmente_no_openmrs_idart.xlsx'),
            col_names = TRUE,
            format_headers = TRUE
          )
          logsExecucao <<- logsExecucao[which(!( logsExecucao$uuid %in% pacintes_erro_sql$uuid)),]
        } 
        
        
        write_xlsx(
          logsExecucao,
          path = paste0('output/',us.name, ' - Log_de_actualizacoes_feitas.xlsx'),
          col_names = TRUE,
          format_headers = TRUE
        )
        
        save(list = ls(),file =gsub(pattern = ' ', replacement = '_',x = paste0('output/',us.name, '.RData') ))
        # >Zip all files
        zip(zipfile = gsub(pattern = ' ', replacement = '_',x = paste0('zip_',us.name)), files = dir() )
      } 
      

      

