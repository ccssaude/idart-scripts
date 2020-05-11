
#' Actualiza Nids na tabela Patient iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name]  Informacao do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' actualizaUuidIdart(con_idart, [id,uuid,patientid,openmrs_patient_id,full.name] )
#' 
actualizaUuidIdart <-   function(con.idart, patient.to.update) {
  
  idart <- tryCatch({
    
    message(paste0( "iDART - Actualizando uuid do paciente: ",patient.to.update[3] , " para uuid:", patient.to.update[2] ) )
    

    dbExecute(
      con.postgres,
      paste0(
        "update  public.patient set uuidopenmrs = '",
        patient.to.update[2],"' , uuid = '",   patient.to.update[2],"' where id = ",
        as.numeric(patient.to.update[1]),     " ;"
      )
    ) 
    

    logAction(patient.info = patient.to.update,action = paste0('iDART  Paciente:',patient.to.update[3], ' foi actualizado com novo uuid : ', patient.to.update[2] ))
    
  },
  error = function(cond) {
    
    msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o NID:  ",patient.to.update[3], 
           "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")

    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  idart
  
}

#' Actualiza patientid/patientfirstname/patientlastname na tabela Patient/public.patientidentifier/packagedruginfo iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name,index,given_name+midle_name,family_name,firstnames,lastname] 
#'                          Informacao do paciente a ser actualizado.
#'                          
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' updatePackagedrugInfoTmp (con_idart, [id,uuid,patientid,openmrs_patient_id,full.name,index,given_name+midle_name,family_name,firstnames,lastname] , '0111106701/2012/00344') )
#' 
updatePackagedrugInfoTmp<-   function(con.idart, patient.to.update, new.nid) {
  
  idart <- tryCatch({
    
    # A tabela packedruginfotemp e uma tabela de descarga apenas
    dbExecute(
      con.idart,
      paste0(
        "update  public.packagedruginfotmp set patientid = '",
        new.nid,
        "' , patientfirstname = '",
        patient.to.update[7], 
        "' , patientlastname = '",
        patient.to.update[8],
        "' where patientid = '",
        patient.to.update[3],  "' and lower(patientfirstname) = '",
        patient.to.update[9] ,
        "'  and  lower(patientlastname) = '" ,
        patient.to.update[10],
        "' ;") 
    )
    
    # 
    # logAction(patient.info = patient.to.update,action = paste0('iDART- public.patientidentifier:',patient.to.update[3], ' - ',patient.to.update[5],
    #                                                            ' na tabela public.patientidentifier  mudou para: ',new.nid, ' - ',    gsub(pattern = ' ', replacement = '',x =   patient.to.update[7]), ' ',patient.to.update[8]))
    # 
    return(1)
  },
  error = function(cond) { # TODO Lista de pacientes com erro quando se tentou actualizar, escrever um excell
  
    msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o NID:  ",patient.to.update[3], 
                   "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(0)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  return(idart)
  
}


#' Actualiza Nids na tabela Patient/public.patientidentifier/packagedruginfo iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name]  Informacao do paciente a ser actualizado.
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' pat <- c('1,'32435sd-3435sd-35353-wvadfw2-43gf54',0111030701,,'Agostionho Banze)
#' actualizaNidiDART(con_idart, pat ,'0111030701/2016/00087')
#' 
actualizaNidiDART <-   function(con.idart, patient.to.update,new.nid) {
  
  idart <- tryCatch({
    
    message(paste0( "iDART - Actualizando dados do paciente: ",patient.to.update[3] , ' - ',patient.to.update[5], " para NID:", new.nid ) )
    
    dbExecute(con.idart,    paste0("update  public.patientidentifier set value ='",  new.nid,   "' where patient_id = ",
        as.numeric(patient.to.update[1]),
        " ;"
      )
    )
    dbExecute(
      con.idart,
      paste0(
        "update  public.patient set patientid = '",
        new.nid,
        "' where id = ",
        as.numeric(patient.to.update[1]),
        " ;"
      )
    ) 
    # A tabela packedruginfotemp e uma tabela de descarga apenas
    dbExecute(
      con.idart,
      paste0(
        "update  public.packagedruginfotmp set patientid = '",
        new.nid,
        "' where patientid = '",
        patient.to.update[3],  "' and lower(patientfirstname) = '",
              patient.to.update[7] ,
         "'  and  lower(patientlastname) ='" ,
        patient.to.update[8],
        "' ;") )
      
    
    logAction(patient.info = patient.to.update,action = paste0('iDART- NID:',patient.to.update[3], ' - ',patient.to.update[5],
                                                               ' na tabela patient/public.patientidentifier/public.packagedruginfotmp  mudou para: ',new.nid))
    
    return(1)
  },
  error = function(cond) {
  msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o NID:  ",patient.to.update[3], 
                             "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(0)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  return(idart)
  
}


#' Actualiza UUID, NID, Nome & Apelido na tabela Patient/public.patientidentifier/packagedruginfo iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector c(id,uuid,patientid,openmrs_patient_id,full.name,index,given_name,family_name) Informacao do paciente a ser actualizado.
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' pat <- c('1,'32435sd-3435wvadfw2-43gf54',0111030701,,'Agostionho Banze)
#' updatePatientIdart(con_idart, pat ,'0111030701/2016/00087') 
#' 
updatePatientIdart <-   function(con.idart, patient.to.update,new.nid) {
  
  idart <- tryCatch({
    
    message(paste0( "iDART - Actualizando dados do paciente: ",patient.to.update[3] , 
                    ' - ',patient.to.update[5], " para NID:", new.nid, ' - ',  
                  gsub( pattern = 'NA', replacement = ' ',x = paste0( patient.to.update[7], ' ',patient.to.update[8])  ) ))
    
    dbExecute(con.idart,    paste0("update  public.patientidentifier set value ='",  new.nid,   "' where patient_id = ",
                                   as.numeric(patient.to.update[1])," ;"  )   )
    
    dbExecute(  con.idart,
      paste0(
        "update  public.patient set patientid = '",
        new.nid,
        "' , uuid = '",
        patient.to.update[2],
        "' , firstnames = '",
        patient.to.update[7], 
        "' , lastname = '",
        patient.to.update[8],
        "' where id = ",
        as.numeric(patient.to.update[1]),
        " ;"
      )
    ) 
    # A tabela packedruginfotemp e uma tabela de descarga apenas
    dbExecute(
      con.idart,
      paste0(
        "update  public.packagedruginfotmp set patientid = '",
        new.nid,
        "' , patientfirstname = '",
        patient.to.update[7], 
        "' , patientlastname = '",
        patient.to.update[8],
        "' where patientid = '",
        patient.to.update[3],  "' and lower(patientfirstname) = '",
        patient.to.update[9] ,
        "'  and  lower(patientlastname) = '" ,
        patient.to.update[10],
        "' ;") 
      )
    
    
    logAction(patient.info = patient.to.update,action = paste0('iDART- NID:',patient.to.update[3], ' - ',
                                                               patient.to.update[5],
                                                               ' na tabela patient/public.patientidentifier/public.packagedruginfotmp  mudou para: ',
                                                               new.nid, ' - ',  gsub(pattern = 'NA',replacement = ' ',x = paste0(patient.to.update[7],
                                                                ' ',patient.to.update[8])) ) )
    
    return(1)
  },
  error = function(cond) { # TODO Lista de pacientes com erro quando se tentou actualizar, escrever um excell

    
    msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o NID:  ",patient.to.update[3], 
                   "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")
    
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(0)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  return(idart)
  
}



#' Actualiza Nomes & NIds na tabela Patient/Patient identifier & packagedrug_info_temp.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name]  Informacao do paciente a ser actualizado.
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' pat <- c('1,'32435sd-3435sd-35353-wvadfw2-43gf54',0111030701,,'Agostionho Banze)
#' actualizaNidiDART(con_idart, pat ,'0111030701/2016/00087')
#' 
actualizaNidNomeiDART <-   function(con.idart, patient.to.update,new.nid) {
  
  message(paste0( "iDART - Actualizando dados do paciente: ",patient.to.update[3] , ' - ',patient.to.update[5],
                  " para NID:", new.nid, ' - ',    gsub(pattern = 'NA', replacement = ' ',x =   paste0(patient.to.update[7], 
                  ' ',patient.to.update[8] ) ) ) )
  
  idart <- tryCatch({
    

    dbExecute(
      con.idart,
      paste0(
        "update  public.patientidentifier set value ='",
        new.nid,
        "' where patient_id = ",
        as.numeric(patient.to.update[1]),
        " ;"
      )
    )
    dbExecute(
      con.idart,
      paste0(
        "update  public.patient set patientid = '",
        new.nid,
        "' , firstnames = '",
        patient.to.update[7],
        "' , lastname = '",
        patient.to.update[8],
        "' where id = ",
        as.numeric(patient.to.update[1]),
        " ;"
      )
    ) 
    # A tabela packedruginfotemp e uma tabela de descarga apenas
    dbExecute(
      con.idart,
      paste0(
        "update  public.packagedruginfotmp set patientid = '",
        new.nid,
        "' , patientfirstname = '",
        patient.to.update[7], 
        "' , patientlastname = '",
        patient.to.update[8],
        "' where patientid = '",
        patient.to.update[3],  "' and lower(patientfirstname) = '",
        patient.to.update[9] ,
        "'  and  lower(patientlastname) = '" ,
        patient.to.update[10],
        "' ;") 
    )
    
    

    logAction(patient.info = patient.to.update,action = paste0('iDART- NID:',patient.to.update[3], ' - ',
                                                               patient.to.update[5],
                                                               ' na tabela patient/public.patientidentifier/public.packagedruginfotmp  mudou para: ',
                                                               new.nid, ' - ',  gsub(pattern = 'NA',replacement = ' ',x = paste0(patient.to.update[7],
                                                                                                                                 ' ',patient.to.update[8])) ) )
    
    return(1)
  },
  error = function(cond) {
    
    msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o NID:  ",patient.to.update[3], 
                   "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(0)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  return(idart)
  
}

#' Actualiza Nids na tabela patientidentifier OpenMRS
#' 
#' @param con.openmrs conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name]  / [id,uuid,patientid,openmrs_patient_id,full.name,index,given_name+midle_name,family_name,firstnames,lastname] Informacao do paciente a ser actualizado.
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return TRUE/FALSE
#' @examples
#' pat <- c('1,'32435sd-3435sd-35353-wvadfw2-43gf54',0111030701,,'Agostionho Banze)
#' actualizaNidOpenMRS(con_idart, pat ,'0111030701/2016/00087')
#'
actualizaNidOpenMRS <-   function(con.openmrs, patient.to.update,new.nid) {
  

  openmrs <- tryCatch({
    
    message(paste0( "OpenMRS - Actualizando dados   do paciente: ",patient.to.update[3] , ' - ',patient.to.update[5], " para NID:", new.nid ) )
    

  dbGetQuery(con.openmrs, paste0( "update  patient_identifier set identifier ='",
                                    new.nid, 
                                    "' where patient_id = "
                                    ,as.numeric(patient.to.update[4]),
      " ;"
    ) )
   
    
    
    logAction(patient.info = patient.to.update,action = paste0('OpenMRS NID:',patient.to.update[3], ' - ',patient.to.update[5], 
                                                               ' na tabela patient_identifier mudou para: ',new.nid))  
    
    # Clear the result.
    #dbClearResult(rs)

    return(TRUE)
    
    
  },
  error = function(cond) {
    msg <- paste0(" BD_ERROR - OpenMRS Tabela patient_identifier nao foi possivel Actualizar o NID: ",patient.to.update[3], 
                   "- ",patient.to.update[5], " Para : ", new.nid, " Erro: ")
    
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  openmrs
  
}



#' Extract  o numero de sequencia do NID
#' 
#' @param NID do paciente
#' @return  Numero de sequencia do NID
#' @examples getNumSeqNid(0111030701/2010/195) return '00195'
getNumSeqNid <- function(nid){
  
  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  

  if (count == 1) {
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8,9)) {
      ## nids 77/13 , 980/11,  2207/10, 790/08 
    
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      seq <- substr(new_nid, 0, first_index-1 )
      seq <- formatSequencia(seq)
      return(seq)
  } else {
    return(0) ##  nao e possivel econtrar o numSeq
      }
    } else if (count == 2) {
    if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {
      
      secon_index <- stri_locate_last(new_nid, regex = "/")[[1]]
      seq <- substr(new_nid, secon_index + 1, nchar(new_nid))
      seq <- formatSequencia(seq)
      return(seq)
    }else {
      return(0) ##  nao e possivel econtrar o numSeq
    }
    
  } else {
    return(0) ##  nao e possivel econtrar o numSeq
    
  }

}

#'  Extract o ano do  NID
#' 
#' @param NID do paciente
#' @return   Ano do Nid
#' @examples getAnoNid(0111030701/2010/00195)
getAnoNid <- function(nid){
  
  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  
  
  if (count == 1) {
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8,9)) {
      ## nids 77/13 , 980/11,  2207/10, 790/08 
      
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      ano <- substr(new_nid, first_index+1, nchar(new_nid) )
      ano <- formatAno(ano)
      if(evaluateAno(ano)){
        return(ano)
      } else {
        return(0)
      }
      
    
    }else {
      return(0) ##  nao e possivel econtrar o ano
       }
    } else if (count == 2) {
      if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {   # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
        first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
        secon_index <- stri_locate_last(new_nid, regex = "/")[[1]]
        ano <- substr(new_nid,first_index+1, secon_index - 1)
        ano <- formatAno(ano)
        if(evaluateAno(ano)){
          return(ano)
        } else {
          return(0)
        }
      }else {
        return(0) ##  nao e possivel econtrar o ano
      }
      
    } else {
      return(0) ##  nao e possivel econtrar o ano
      
    }
  
}


#' Busca a proxima sequencia no grupo de NIDs organizados por Ano, e por NumSequencia
#' 
#' @param NID do paciente
#' @return  Numero prox Sequencia  
#' @examples getProxSequenciaNid(0111030701/2010/00195)
getProxSequenciaNid <- function(nid,df.openmrs.patients){
  
  seq_actual <- getNumSeqNid(nid)
  
  if(seq_actual!=0){
    df <- df.openmrs.patients
    df$identifier <- sapply(df$identifier,removeLettersFromNid)
    df$ano <- sapply(df$identifier,getAnoNid)
    df$seq <- sapply(df$identifier,getNumSeqNid)
    year <- getAnoNid(nid)
    
    ultima_seq <- df %>% 
      group_by(ano) %>%
      filter(ano == year,seq==max(seq)) %>% pull(seq)
      prox <- as.numeric(ultima_seq) + 1
    
    rm(df)
    
    return(formatSequencia(prox))
  } else {
    return(0)
  }

  
  
  
}

#' Busca o cod da US do NID
#' 
#' @param NID do paciente
#' @return  uusCodea  
#' @examples getUsCode(0111030701/2010/00195)
getUsCode <- function(nid){

  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  
  
  if (count == 1) {
    if (getNidLength(new_nid) %in% c(3, 4, 5, 6, 7, 8)) {
      ## nids 77/13 , 980/11,  2207/10, 790/08 
      return(us.code)
      
    }else {
      return(us.code) ##  CodUS desconhecido 
    }
  } else if (count == 2) {
    if (getNidLength(new_nid) %in% c(14,15, 16, 17,18,19,20,21)) {
     
      
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      codUS <- substr(new_nid,0, first_index - 1)
      codUS <- formatUsCode(codUS)
      return(codUS)
    }else {
      return(0) ##  nao e possivel econtrar o codUS
    }
    
  } else {
    return(0) ##  nao e possivel econtrar o CodUS
    
  }
  
  
  
}

#' Formata a o codigod da US 
#' 
#' @param NID do paciente
#' @return  usCode  
#' @examples formatUsCode(01110307)
#' Retorna 0111030701
formatUsCode <- function(codUS) {

    if (nchar(codUS) == 10) {
    return(codUS)
  } else if (nchar(codUS) == 6) {
    newCod <- paste0("01", codUS,"01")
    return(newCod)
  } else  if (nchar(codUS) == 8) {
    newCod <- paste0("01", codUS)
    return(newCod)
  } else  if (nchar(codUS) == 7) {
    newCod <- paste0("01", substr(codUS,0,2),"0",substr(codUS,3,nchar(codUS)))
    return(newCod)
  } else  if (nchar(codUS) == 9) {
    if(substr(codUS,0,1)!='1'){
      codUS <- paste0('1',substr(codUS,2,nchar(codUS)))
    }
    newCod <- paste0("0", codUS)
    return(newCod)
  } else {
    return(0)
  }
  
}


#' Formata a o codigo da US / Somente para Polana Canico ( pacientes com NIDs sem barras )
#' 
#' @param NID do paciente
#' @return  usCode  
#' @examples formatUsCodeV1(01110307)
#' Retorna 0111030701
formatUsCodeV1 <- function(codUS) {
  
  if (nchar(codUS) == 10) {
    return(codUS)
  } else if (nchar(codUS) == 6) {
    newCod <- paste0("01", codUS,"01")
    return(newCod)
  } else  if (nchar(codUS) == 8) {
    if( "01" == substr(codUS,0,2)){
      newCod <- paste0( codUS,"01")
    } else {
      newCod <- paste0("01", codUS)
    }
   
    return(newCod)
  } else  if (nchar(codUS) == 7) {
    newCod <- paste0("01", substr(codUS,0,2),"0",substr(codUS,3,nchar(codUS)))
    return(newCod)
  } else  if (nchar(codUS) == 9) {
    if(substr(codUS,0,1)!='1'){
      codUS <- paste0('1',substr(codUS,2,nchar(codUS)))
    }
    newCod <- paste0("0", codUS)
    return(newCod)
  } else {
    return(0)
  }
  
}

formatNidMisau <- function(nid) {
  
  new_nid <- removeLettersFromNid(nid)
  if(checkNidFormatMisau(new_nid)){
    
    return(new_nid)
    
  } else {
   
    if (!checkBarraNid(new_nid)){
      
      return(0)
    } else {
      
      count <- str_count(new_nid, '/')  # Quantas barras tem o nid
      
      if (count == 1) {
        if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8)) {
          ## nids  4,5,6, 7,8)
          
          if(0 != getUsCode(new_nid)){ 
            if(0 != getAnoNid(new_nid)){
              nid_reformatado <- formatNidUmaBarra(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{ return(0) } }
        else {
          return(0) ## Nao foi possivel formatar o nid
        }
        
      } 
      else if (count == 2) {
        if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {       # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
          
          if(0 != getUsCode(nid)){ 
            if(0 != getAnoNid(nid)){
              nid_reformatado <- formatNidDuasBarras(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{return(0)}
        } 
        else if(getNidLength(new_nid)==21){ #  acho k e reduntande ja que veridacamos seo nidetem 21caracteres na funcao checkNidFormatMisau(new_nid)
          return(new_nid)
        } else{ return(0) }
        
      } 
      else {
        return(0) ##  nao e possivel reformatar o nid com script deve ser manualemnte}
        
      }
      
    }
      
    } 
  
}

#' Retorna o tamanho  do NID 
#' 
#' @param nid do paciente
#' @return  integer  
#' @examples getNidLength('0111030701/2010/00195')
getNidLength <- function(nid) {
  nchar(nid)
}

#formata nids com 4,5,6,7 e 8 caracteres

formatNidUmaBarra <- function(nid) {
  
   
     nid_formatado <-
      paste0(getUsCode(nid), "/", getAnoNid(nid), "/", getNumSeqNid(nid))
     nid_formatado # The same as return(nid_formatado)
   
  
}

#formata nids com 15,16,17 caracteres

formatNidDuasBarras <- function(nid) {
  nid_formatado <-
    paste0(getUsCode(nid), "/",  getAnoNid(nid), "/", getNumSeqNid(nid))
  nid_formatado # The same as return(nid_formatado)
}

#' Atribui um novo nid apartir do antigo, Troca o codigo de servico
#' se for 01 troca para 02
#' @param old.nid nid  do paciente que se vai atribuir novo nid
#' @return  novo nid  
#' @examples getNewNidNewSeq(0111030701/2010/00195) retorna 01111030702/2010/(00195)
#' @examples getNewNidNewSeq( 2145/11) retorna 01111030701/2011/2145
getNewNidNewSeq <- function(old.nid, df.openmrs.patients) {
  
  prox_seq <- getProxSequenciaNid(old.nid,df.openmrs.patients)
  
  us_code <- getUsCode(old.nid)
  year <- getAnoNid(old.nid)
  if(prox_seq!=0){
    
    if(0 != us_code){
      
      
      if(0!= year){
        
        new_nid <-
        paste0(us_code, "/",  year, "/", prox_seq)
        new_nid  # The same as return(nid_formatado)
      }else {return(0)}
    
    } else {return(0)}

  } else{ return(0)}

}

#' Atribui um novo nid apartir do antigo, Troca o codigo de servico
#' se for 01 troca para 02. se estiver incompleto formata
#' @param old.nid nid  do paciente que se vai atribuir novo nid
#' @return  novo nid  
#' @examples getNewNidNewSeq(0111030701/2010/00195) retorna 01111030702/2010/(00195)
#' @examples getNewNidNewSeq( 2145/11) retorna 01111030701/2011/2145
getNewNid <- function(old.nid) {
  
  antigo <- old.nid
  new_nid <- formatNidMisau(antigo)
  
  if(0 != new_nid) {
    
    if(new_nid ==antigo ){
      #TODO mudar o codigdo de servico
      us_code <- getUsCode(new_nid)
      new_us_cod <- changeUsCode(us_code)
      nid_formatado <-
        paste0(new_us_cod, "/",  getAnoNid(new_nid), "/", getNumSeqNid(new_nid))
      return(nid_formatado) # The same as return(nid_formatado)
    } else { 
      
      return(new_nid)
      }
    
  } else { return(0)}
  

}

#' Atribui um novo nid apartir do antigo,  trocando o cod de servico ou formatando o nid obdecendo o 
#' padrao do MISAU -> (Valido para US com nids no formato ano/seq : ex 12/384 ,06/2357)
#' @param old.nid nid  do paciente que se vai atribuir novo nid
#' @return  novo nid  
#' @examples getNewNid(10/195) retorna 0111030702/2010/00195
#' @examples getNewNid( 11/34455) retorna 01111030701/2011/2145
getNewNidV1 <- function(old.nid) {
  antigo = old.nid
  new_nid <- formatNidMisauV1(antigo)
  
  if(0 != new_nid) {
    
  if(new_nid ==antigo ){
    #TODO mudar o codigdo de servico
    us_code <- getUsCode(new_nid)
    new_us_cod <- changeUsCode(us_code)
    nid_formatado <-
      paste0(new_us_cod, "/",  getAnoNidV1(new_nid), "/", getNumSeqNidV1(new_nid))
    return(nid_formatado) 
  } else { 
    
    return(new_nid)
  } }  else {
    return(0)
  }
  
}


#' Verifica se o nid existe na BD iDART
#' @param nid nid  do paciente que se vai atribuir novo nid
#' @param df dataframe onde se faz a verificacao  
#' @return  TRUE/FALSE
#' @examples checkIfExistsNiD(0111030701/2010/00195,df) retorna TRUE se nid existe em df

checkIfExistsNidOpenMRS <- function(nid,df) {
  
  if(nid %in% df$identifierSemLetras){
    return(TRUE)
  }else{
    return(FALSE)
  }
  
} 
#' Verifica se o nid existe ns BD OpenMRS
#' @param nid nid  do paciente que se vai atribuir novo nid
#' @param df dataframe onde se faz a verificacao  
#' @return  TRUE/FALSE
#' @examples checkI
checkIfExistsNidIdart <- function(nid,df) {
  
  if(nid %in% df$patientidSemLetras){
    return(TRUE)
  }else{
    return(FALSE)
  }
  
    
} 

# Verifica se NID tem barra
checkBarraNid <- function(nid) {
  if (!grepl(pattern = "/", nid))
  {
    #print(paste0("O nid ", nid, " nao contem barra '/' "))
    return(FALSE)
  }
  return(TRUE)
}

## Se existir um caracter no ano deve-se remover

formatAno <- function(ano) {
  if (nchar(ano) == 3) {
    newYear <- paste0("2", ano)
    return(newYear)
  } else  if (nchar(ano) == 2) {
    newYear <- paste0("20", ano)
    return(newYear)
  } else  if (nchar(ano) == 1) {
    newYear <- paste0("200", ano)
    return(newYear)
  } else  if (nchar(ano) == 4) {
    return(ano)
  } else {
    return(0)
  }
  
}

formatSequencia <- function(seq) {
  if (nchar(seq) == 1) {
    newSequencia <- paste0("0000", seq)
    return(newSequencia)
  } else  if (nchar(seq) == 2) {
    newSequencia <- paste0("000", seq)
    return(newSequencia)
  } else  if (nchar(seq) == 3) {
    newSequencia <- paste0("00", seq)
    return(newSequencia)
  } else  if (nchar(seq) == 4) {
    newSequencia <- paste0("0", seq)
    return(newSequencia)
  }  else  if (nchar(seq) == 5) {
    return(seq)
  }else  if (nchar(seq) == 6) {
    newSequencia <- substr( seq, 2,nchar(seq))
    return(newSequencia)
  } else {
    return(0)
  }
  
}

#' Remove letras dos nids dos pacientes
#' 
#' @param nid  NID  do paciente
#' @return  NID  sem Letras  
#' @examples nid <- removeLettersFromNid('12/345 SAAJ')
removeLettersFromNid <- function(nid) {
  nid <-
    gsub(" ", "", nid, fixed = TRUE)  # remover espacos em branco do nid
  nid <-
    gsub("[A-z]", "", nid)           # remover caracteres do nid
  nid <-
    gsub("[:punct:]", "", nid)        # remover Punctuation character: do nid: ! " # $ % & ' ( ) * + ,  ~
  nid <-
    gsub("-", "", nid)  
  nid <- gsub(x = nid,pattern = "\\*",replacement = '')   # remover  *
  nid <- gsub(x = nid,pattern = "'",replacement = '')   # remover  *
  nid
  
  
}

#' Remove Numeros dos nomes dos pacientes
#' 
#' @param name nome  do paciente
#' @return  name sem numeros  
#' @examples name <- removeNumbersFromName('Agnaldo 5-45j samuel)
removeNumbersFromName <- function(name) {

  
  name <-   gsub("[0-9]", "", name)           # remover numeros
  name <-   gsub("-", "", name)  
  name <-   gsub("\\*", "", name)  
  name <-   gsub("\\(", "", name)  
  name <-   gsub("\\)", "", name)  
  name
}

checkNidFormatMisau <- function(nid) {
  if (checkBarraNid(nid)) {
    count <- str_count(nid, '/')  # Quantas barras tem o nid
    
    if (count == 1) {
      
      return(FALSE)
    }
    else if(count == 2) {
      if (getNidLength(nid) ==21)
      {
        return(TRUE)}
      
      else{
        return(FALSE)
      } 
      
    } 
    else {
      return(FALSE) 
      
    }
  }  else {
    return(FALSE) 
    
  }
}


#' Verifica se o ano esta dentro dos padroe do  do NID 
#' 
#' @param an do paciente
#' @return  TRUE/FALSE  
#' @examples evaluateAno(2030)
evaluateAno<- function(year){
  
  if((as.numeric(year)>2000) & (as.numeric(year)<=2020)){
    
    return(TRUE)
  }else {
    return(FALSE)
  }
}



#' Modifica o codigo de servico de um codigo da US, troca  de 1 para 2 e vice versa
#' 
#' @param us.code  cod da us do nid
#' @return  Novo cod da us  
#' @examples changeUsCode('0111030701')
changeUsCode<- function(us.code){
  
  first <- substr(us.code, 1, nchar(us.code)-1)
  last <- substr(us.code, nchar(first)+1, nchar(us.code))
  if(last==1){
    last = 2
    new_us_cod <- paste0(first,last)
    new_us_cod
  } else if (last==2){
    last = 1
    new_us_cod <- paste0(first,last)
    new_us_cod
  } else {
    new_us_cod <- paste0(first,as.numeric(last)+1)
    new_us_cod
  }
}





######################## FormatNidMisauV1  ###################################################
# Para US que os nids mal formatados comecam com o ano: ex 10/3003 , 09/345 , 16/9356


formatNidMisauV1 <- function(nid) {
  
  new_nid <- removeLettersFromNid(nid)
  if(checkNidFormatMisau(new_nid)){
    
    return(new_nid)
    
  } else {
    
    if (!checkBarraNid(new_nid)){
      
      return(0)
    } else {
      
      count <- str_count(new_nid, '/')  # Quantas barras tem o nid
      
      if (count == 1) {
        if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8,9)) {
          ## nids  4,5,6, 7,8)
          
          if(0 != getUsCode(nid)){ 
            if(0 != getAnoNidV1(nid)){
              nid_reformatado <- formatNidUmaBarraV1(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{ return(0) } }
        else {
          return(0) ## Nao foi possivel formatar o nid
        }
        
      } 
      else if (count == 2) {
        if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {       # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
          
          if(0 != getUsCode(nid)){ 
            if(0 != getAnoNidV1(nid)){
              nid_reformatado <- formatNidDuasBarrasV1(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{return(0)}
        } 
        else if(getNidLength(new_nid)==21){ #  acho k e reduntande ja que veridacamos seo nidetem 21caracteres na funcao checkNidFormatMisau(new_nid)
          return(new_nid)
        } else{ return(0) }
        
      } 
      else {
        return(0) ##  nao e possivel reformatar o nid com script deve ser manualemnte}
        
      }
      
    }
    
  } 
  
}


#' Extract  o numero de sequencia do NID
#' 
#' @param NID do paciente
#' @return  Numero de sequencia do NID
#' @examples getNumSeqNidV1(0111030701/2010/195) return '00195'
getNumSeqNidV1 <- function(nid){
  
  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  
  
  if (count == 1) {
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8,9)) {
      ## nids 13/77 , 11/980,  10/2207, 08/790 
      
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      seq <- substr(new_nid,  first_index+1 , nchar(new_nid))
      seq <- formatSequencia(seq)
      return(seq)
    } else {
      return(0) ##  nao e possivel econtrar o numSeq
    }
  } else if (count == 2) {
    if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {
      
      secon_index <- stri_locate_last(new_nid, regex = "/")[[1]]
      seq <- substr(new_nid, secon_index + 1, nchar(new_nid))
      seq <- formatSequencia(seq)
      return(seq)
    }else {
      return(0) ##  nao e possivel econtrar o numSeq
    }
    
  } else {
    return(0) ##  nao e possivel econtrar o numSeq
    
  }
  
}

#'  Extract o ano do  NID  para nids formatados da seguinte fomra 10/2233 9/3945  2012/324958()
#' 
#' @param NID do paciente
#' @return   Ano do Nid
#' @examples getAnoNidV1(0111030701/2010/00195)
getAnoNidV1 <- function(nid){
  
  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  
  
  if (count == 1) {
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8,9)) {
      ## nids 13/77 , 11/980,  10/2207, 08/790 
      
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      ano <- substr(new_nid, 0,  first_index -1 )
      ano <- formatAno(ano)
      if(evaluateAno(ano)){
        return(ano)
      } else {
        return(0)
      }
      
      
    }else {
      return(0) ##  nao e possivel econtrar o ano
    }
  } else if (count == 2) {
    if (getNidLength(new_nid) %in% c(13,14,15, 16, 17,18,19,20,21)) {   # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      secon_index <- stri_locate_last(new_nid, regex = "/")[[1]]
      ano <- substr(new_nid,first_index+1, secon_index - 1)
      ano <- formatAno(ano)
      if(evaluateAno(ano)){
        return(ano)
      } else {
        return(0)
      }
    }else {
      return(0) ##  nao e possivel econtrar o ano
    }
    
  } else {
    return(0) ##  nao e possivel econtrar o ano
    
  }
  
}


#formata nids com 4,5,6,7 e 8 caracteres

formatNidUmaBarraV1 <- function(nid) {
  
  
  nid_formatado <-
    paste0(getUsCode(nid), "/", getAnoNidV1(nid), "/", getNumSeqNidV1(nid))
  nid_formatado # The same as return(nid_formatado)
  
  
}

#formata nids com 15,16,17 caracteres

formatNidDuasBarrasV1 <- function(nid) {
  nid_formatado <-
    paste0(getUsCode(nid), "/",  getAnoNidV1(nid), "/", getNumSeqNidV1(nid))
  nid_formatado # The same as return(nid_formatado)
}





#' Verifica se um determinado nid formatado existe na BD OpenMRS/iDART
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' status = checkNidExistsOpenmrsIdart('0111040201/2019/20384')
checkNidExistsOpenmrsIdart <- function (nid){
  if(nid %in% idartAllPatients$patientid){
    if(nid %in% openmrsAllPatients$identifier){
      return("existe_openmrs_idart")
    }else{     
      return("existe_idart")}

  }else if(nid %in% openmrsAllPatients$identifier){
    return("existe_openmrs")
  }else {
    return("nao_existe")
  }
  
}
