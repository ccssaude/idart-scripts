#############################################  Para cada grupo de duplicados (NID) definir Solucao  #########################################################
#############################################  G 1,2,3,4 - Categorias de duplicados  ######################################################################### 
#############################################  CM - Corrigir Manualmente             #########################################################################
#############################################  CC - Corrigir atraves de codigo       #########################################################################
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
################################  Grupo 4 (G4): Pacientes  Triplicados############                                                                ############ 
############ 
## Grupo   4.1 (G4.1):  - Dos 3 pacientes 2 tem dados iguais                                                                                      ############ 
## Solucao 4.1 (G4.1):-CM  Unir  os paciente com os dados iguais                                                                                     ############ 
############ 
##############################################################################################################################################################
##############################################################################################################################################################



source('paramConfiguration.R')                     ## Carregar as configuracoes





      ## Pacientes
      ## Buscar todos pacientes OpenMRS & iDART
      openmrsAllPatients <- getAllPatientsOpenMRS(con_openmrs)
      openmrsAllPatients$identifierSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   
      idartAllPatients <- getAllPatientsIdart(con_postgres)
      idartAllPatients$patientidSemLetras <- sapply(idartAllPatients$patientid, removeLettersFromNid)    
      openmrsAllPatients$patientidSemLetras <- sapply(openmrsAllPatients$identifier, removeLettersFromNid)   
      
      
      # Busca o codigo da US
      us.code= getOpenmrsUsCode(openmrsAllPatients)                 
      
      
      ## Pacientes Duplicados Por NID iDART & OpenMRS
      duplicadosOpenmrs <- getDuplicatesPatOpenMRS(con_openmrs)
      duplicadosiDART <-   getDuplicatesPatiDART(con_postgres)
      
      
      # Cruzar duplicados iDART  com dados do openmrs
      dups_idart_openmrs <- inner_join(duplicadosiDART,openmrsAllPatients, by=c("uuid"))
      dups_idart_openmrs$solucao <- ""
 
##############################################################################################################################################################
##############################################################################################################################################################
      ##  Correcao automatica dos  nids
      nidsAllDupsPatients <- unique(dups_idart_openmrs$patientid)
      
      for (i in 1:length(nidsAllDupsPatients)) {
        
        nid_duplicado <- nidsAllDupsPatients[i]
        index <- which(dups_idart_openmrs$patientid==nid_duplicado)
        
        
        if(length(index)==2){
          
          if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){  ## Grupo 1 (G1)
            # Solucao Grupo 1 (G1.1)
            solucao <- paste0("G1.1-CM  Unir  os paciente com o NID: ", dups_idart_openmrs$patientid[index[1]] , "no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
            logSolucao(index[1],index[2] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
          } 
          
          else { 
            
            nome_pat_1 <- dups_idart_openmrs$full_name[index[1]]
            nome_pat_2 <- dups_idart_openmrs$full_name[index[2]]
            
            estado_tarv_1 <- dups_idart_openmrs$estado_tarv[index[1]]
            estado_tarv_2 <- dups_idart_openmrs$estado_tarv[index[2]]
            #################################    Grupo 2 (G2)   ###########################################
            #  Avaliar o grau de simetria dos nomes , para saber se trata-se do mesmo paciente 
            #Algoritimo  de simetria de strings com o method :  Jaro-Winker distance (eficiente para comparacao de nomes)
            if(stringdist(nome_pat_1,nome_pat_2, method = "jw") > 0.15){     # Grupo 2 (G2)
              #s e o rsultado de stringdist for:
              # 0 - perfect match, 0.5 - minimo aceitavel definido, 1 - no match at al
              if(! (is.na(estado_tarv_1) | is.na(estado_tarv_2)) ) {
                
                # Grupo 2.1 (G2.1) Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao abandonos 
                # Solucao G2.1:-CC  
                if(estado_tarv_1 == estado_tarv_2 & estado_tarv_2=='ABANDONO'){ 
                  
                  patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado)
                  new_nid = 0
                  
                  if(tipo_nid=='seq/ano'){
                    new_nid <- getNewNid(patient_to_update[3])  
                  }else if(tipo_nid=='ano/seq')
                  { new_nid <- getNewNidV1(patient_to_update[3])    }
                  else {
                    message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                     break
                    }

                  if( 0 != new_nid){
                    solucao <- paste0("G2.1:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ",nid_duplicado ,' - ',  patient_to_update[5], " por ser abandono e  ter a data do ult lev menos recente")
                    logSolucao(index[1],index[2] ,solucao)  
                    beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                  } else {  # TODO escrever no log nao foi possivel obter novo nid
                    
                  }
                  
                  
                }  
                else if(estado_tarv_1 != estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){ 
                  
                  ## Grupo   2.2 (G2.2):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que os dois sao activos                      
                  ## Solucao G2.2:-CC      Trocar o nid de um dos pacientes, os dois estao activos                   
                  
                  if( 'TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){  
                    
                    patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado)
                    new_nid = 0
                    
                    if(tipo_nid=='seq/ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='ano/seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                      break
                    }
                    if( 0 != new_nid){
                      solucao <- paste0("G2.2:CC - Pacientes duplicados no iDART e OpenMRS, os 2 sao activos, Trocar o nid  :",nid_duplicado ,' - ',  patient_to_update[5],  ", por ter  a data do ult_lev menos recente") 
                      logSolucao(index[1],index[2],solucao)
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres) # kaka
                    } else{}
                  }
                  else {
                    # Grupo 2.3 (G2.3):  -Pacientes  duplicados  com  uuids diferentes e nomes diferentes sendo que um dos pacientes nao e activo
                    patient_to_update <- getPacInactivo(index=index,nid = nid_duplicado)
                    new_nid = 0
                    
                    if(tipo_nid=='seq/ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='ano/seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                      break
                    }
                    if( 0 != new_nid){
                      solucao <- paste0("G2.3:-CC - Trocar no OpenMRS e iDART o  NID do paciente:",nid_duplicado ,' - ', patient_to_update[5],  "  ele nao e activo em tarv nesta US")
                      logSolucao(index[1],index[2],solucao)
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                    } else{}
                  }
                }
                else  if(estado_tarv_1 == estado_tarv_2 &  'ACTIVO NO PROGRAMA' %in%  c(estado_tarv_1,estado_tarv_2)){
                  #Solucao G2.2:-CC    Trocar o nid de um dos pacientes, os dois estao activos, de preferencia aquele que tiver a data do ult_lev menos recente
                  patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado)
                  new_nid = 0
                  
                  if(tipo_nid=='seq/ano'){
                    new_nid <- getNewNid(patient_to_update[3])  
                  }else if(tipo_nid=='ano/seq')
                  { new_nid <- getNewNidV1(patient_to_update[3])    }
                  else {
                    message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                    break
                  }  
                  if( 0 != new_nid){
                    solucao <- paste0("G2.2:-CC  - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ",nid_duplicado ,' - ', 
                                      patient_to_update[5], " por ter a data do ult lev menos recente")
                    logSolucao(index[1],index[2] ,solucao)  
                    beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                  } else {}
                }
                else {
                  
                  if('TRANSFERIDO DE' %in%  c(estado_tarv_1,estado_tarv_2)){
                    if(estado_tarv_1==estado_tarv_2){ ## os dois pacientes sao activos
                      
                      patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado)
                      new_nid = 0
                      
                      if(tipo_nid=='seq/ano'){
                        new_nid <- getNewNid(patient_to_update[3])  
                      }else if(tipo_nid=='ano/seq')
                      { new_nid <- getNewNidV1(patient_to_update[3])    }
                      else {
                        message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                        break
                      }
                      if( 0 != new_nid){
                        solucao <- paste0("G2.3:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", nid_duplicado ,' - ', 
                                          patient_to_update[5], " por ter a data do ult lev menos recente")
                        logSolucao(index[1],index[2] ,solucao)  
                        beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                      } else{}
                    }
                    else{ 
                      
                      patient_to_update <- getPacInactivo(index,nid = nid_duplicado)
                      new_nid = 0
                      
                      if(tipo_nid=='seq/ano'){
                        new_nid <- getNewNid(patient_to_update[3])  
                      }else if(tipo_nid=='ano/seq')
                      { new_nid <- getNewNidV1(patient_to_update[3])    }
                      else {
                        message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                        break
                      }  
                      if( 0 != new_nid){
                        solucao <- paste0("G2.3:CC - Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS,  de preferencia: ", nid_duplicado ,' - ', patient_to_update[5], " por ter saido do Tarv")
                        logSolucao(index[1],index[2] ,solucao)  
                        beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                      } else{}
                    }} 
                  else{
                    
                    ## Solucao  G2.4:CC -     Trocar o nid de  um dos pacientes  de preferencia o que tiver a data de lev menos recente    
                    patient_to_update <- getPacLevMenosRecente(index,nid = nid_duplicado)
                    new_nid = 0
                    
                    if(tipo_nid=='seq/ano'){
                      new_nid <- getNewNid(patient_to_update[3])  
                    }else if(tipo_nid=='ano/seq')
                    { new_nid <- getNewNidV1(patient_to_update[3])    }
                    else {
                      message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                      break
                    } 
                    if( 0 != new_nid){
                      solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por nao ser activo e ter a data do ult lev menos recente")
                      logSolucao(index[1],index[2] ,solucao)  
                      beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                    }else{}
                  }
                  
                }
                
              }
              else if (is.na(estado_tarv_1) & is.na(estado_tarv_2)) {
                
                # Solucao (G2.3):-CC Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS
                patient_to_update <- composePatientToUpdate(index[1],df = dups_idart_openmrs)
                new_nid = 0
                if(tipo_nid=='seq/ano'){
                  new_nid <- getNewNid(patient_to_update[3])  
                }else if(tipo_nid=='ano/seq')
                { new_nid <- getNewNidV1(patient_to_update[3])    }
                else {
                  message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                  break
                }  
                
                if( 0 != new_nid){
                  solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por ser duplicado e  nao  nao ter o estado de permanecia definido")
                  logSolucao(index[1],index[2] ,solucao)  
                  beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                }else{}
              }
              else if(is.na(estado_tarv_1) | is.na(estado_tarv_2))  {
                
                index_pac_sem_estado_def <- which(is.na(c(estado_tarv_1,estado_tarv_2)))
                
                # Solucao (G2.3):-CC Trocar o nid de um dos pacientes no iDART e tambem no OpenMRS
                patient_to_update <- composePatientToUpdate(index[index_pac_sem_estado_def],df = dups_idart_openmrs)
                new_nid = 0
                if(tipo_nid=='seq/ano'){
                  new_nid <- getNewNid(patient_to_update[3])  
                }else if(tipo_nid=='ano/seq')
                { new_nid <- getNewNidV1(patient_to_update[3])    }
                else {
                  message(" Nao especificou o tipo de NID  (ano/seq   ou seq/ano) no ficheiro de config paramConfiguration.R)")
                  break
                }  
                
                if( 0 != new_nid){
                  solucao <- paste0("G2.2:-CC  - Trocar o nid  : ",nid_duplicado ,' - ',  patient_to_update[5], " por ser duplicado e  nao  nao ter o estado de permanecia definido")
                  logSolucao(index[1],index[2] ,solucao)  
                  beginUpdateProcess(new_nid,patient_to_update,idartAllPatients,openmrsAllPatients,con_openmrs,con_postgres)
                }else{}

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
              
              
              patient_to_update <-    getPacLevMaisRecente(index, nid = nid_duplicado)
              solucao <-  paste0("G3.1:-CM  -  Unir  os paciente com o NID: (", nid_duplicado,' - ' , dups_idart_openmrs$full_name[index[1]] ,
                                 ") , com ",  "(", nid_duplicado,' - ' , dups_idart_openmrs$full_name[index[2]] ,") ,no iDART e no OpenMRS, o preferido e aquele
                           que tiver maior numero de levantamentos" )
              
              logSolucao(index[1], index[2] , solucao)
              logAction(composePatientToUpdate(index[1], df = dups_idart_openmrs),
                        solucao)
            
              
            }
            
          }
        }
        
        else if(length(index)==3){   # Grupo G4
          
          if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[2]] ){
            
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[1]],' - ' ,dups_idart_openmrs$full_name[index[1]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[2]],' - ' ,dups_idart_openmrs$full_name[index[2]] , ") ,no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
            logSolucao(index[1],index[2] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            
            
          } 
          else if(dups_idart_openmrs$uuid[index[1]] == dups_idart_openmrs$uuid[index[3]] ){
            
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[1]],' - ' ,dups_idart_openmrs$full_name[index[1]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[3]],' - ' ,dups_idart_openmrs$full_name[index[3]] , ") ,no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
            logSolucao(index[1],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
            
            
          }
          else  if(dups_idart_openmrs$uuid[index[2]] == dups_idart_openmrs$uuid[index[3]] ){
            
            solucao <- paste0("G4.1-CM  Unir  os paciente com o NID: (", dups_idart_openmrs$patientid[index[2]],' - ' ,dups_idart_openmrs$full_name[index[3]] , ") , com ",
                              "(", dups_idart_openmrs$patientid[index[3]],' - ' ,dups_idart_openmrs$full_name[index[3]] , ") ,no iDART e no OpenMRS, o preferido e aquele  que tiver maior numero de levantamentos")
            logSolucao(index[2],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[2],df = dups_idart_openmrs),solucao  )
          }
          else {
            
            # Solucao Grupo G4.1  (G4.1)
            solucao <- paste0("N/P nao foi possivel encontrar a solucao para os pacientes triplicados. Deve-se corrigir manualmente")
            logSolucao(index[1],index[3] ,solucao)  
            logAction(composePatientToUpdate(index[1],df = dups_idart_openmrs),solucao  )
          }
        }
        
        
      }
      
      
##############################################################################################################################################################
##############################################################################################################################################################
      ##  Exportar os Logs
      if (dim(logsExecucao)[1]>0){
        
        corrigir_manualmente <- logsExecucao[which(grepl(pattern = 'CM',x=logsExecucao$accao,ignore.case = FALSE)),]
        us.name <- getOpenmrsDefaultLocation(con_openmrs) 
        
        write_xlsx(
          corrigir_manualmente,
          path = paste0(us.name,' - Pacientes_para_unir_idart_openmrs.xlsx'),
          col_names = TRUE,
          format_headers = TRUE
        )
        
        
        write_xlsx(
          logsExecucao,
          path = paste0(us.name, ' - log_de_mudancas_idart_e_openmrs.xlsx'),
          col_names = TRUE,
          format_headers = TRUE
        )
        
       
        
      }  # Exportar os logs para excell
      
    
