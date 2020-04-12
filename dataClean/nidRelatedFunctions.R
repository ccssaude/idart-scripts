

#' Actualiza Nids na tabela Patient iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param old.patient.patient.id NID do paciente a ser actualizado.
#' @param patient.id.idart id do paciente a ser actualizado
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' actualizaNidiDART(con_idart, '0111030701/2014/00065',56,'0111030701/2016/00087')
actualizaNidiDART <-   function(con.idart, old.patient.patientid,patient.id.idart,new.nid) {
  
  idart <- tryCatch({
    
    message(paste0( "iDART - Actualizando dados do paciente: ",old.patient.patientid , " para NID:", new.nid ) )
    
    dbExecute(
      con.postgres,
      paste0(
        "update  public.patientidentifier set value ='",
        new.nid,
        "' where patient_id = ",
        patient.id.idart,
        " ;"
      )
    )
    dbExecute(
      con.postgres,
      paste0(
        "update  public.patient set patientid = '",
        new.nid,
        "' where id = ",
        patient.id.idart,
        " ;"
      )
    )
    
    
    
  },
  error = function(cond) {
    message("Nao foi possivel Actualizar o NID do paciente":old.patient.patientid)
    message(cond)
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
  
  idart
  
}

#' Actualiza Nids na tabela patientidentifier OpenMRS
#' 
#' @param con.openmrs conexao com  MySQL- OpenMRS
#' @param old.patient.patient.id NID do paciente a ser actualizado.
#' @param patient.id.openmrs id do paciente a ser actualizado
#' @param new.nid Novo NID do paciente a ser actualizado.
#' @return dataframe with zero legth
#' @examples
#' actualizaNidiDART(con_openmrs, '0111030701/2014/00065',56,'0111030701/2016/00087')
actualizaNidOpenMRS <-   function(con.openmrs, old.patient.patientid,patient.id.openmrs,new.nid) {
  
  openmrs <- tryCatch({
    
    message(paste0( "OpenMRS - Actualizando dados   do paciente: ",old.patient.patientid , " para NID:", new.nid ) )
    
    dbGetQuery(
      con.openmrs,
      paste0(
        "update  patient_identifier set identifier ='",
        new.nid,
        "' where patient_id = ",
        patient.id.openmrs,
        " ;"
      )
    )
    
    
    
    
  },
  error = function(cond) {
    message("Nao foi possivel Actualizar o NID do paciente":patient.patientid)
    message(cond)
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
  
  openmrs
  
}

#' Extract  o numero de sequencia do NID
#' 
#' @param NID do paciente
#' @return  Numero de sequencia do NID
#' @examples getNumSeqNid(0111030701/2010/00195)
getNumSeqNid <- function(nid){
  
  new_nid <- removeLettersFromNid(nid)
  # Quantas barras tem o nid
  count <- str_count(new_nid, '/')  

  if (count == 1) {
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8)) {
      ## nids 77/13 , 980/11,  2207/10, 790/08 
    
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      seq <- substr(new_nid, 0, first_index-1 )
      seq <- formatSequencia(seq)
      return(seq)
  } else {
    return(0) ##  nao e possivel econtrar o numSeq
      }
    } else if (count == 2) {
    if (getNidLength(new_nid) %in% c(15, 16, 17,18,19,20,21)) {
      
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
    if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8)) {
      ## nids 77/13 , 980/11,  2207/10, 790/08 
      
      first_index <- stri_locate_first(new_nid, regex = "/")[[1]]
      ano <- substr(new_nid, first_index+1, nchar(new_nid) )
      ano <- formatAno(ano)
      if(evaluateAno(ano)){
        return(ano)
      } else {
        return(FALSE)
      }
      
    
    }else {
      return(FALSE) ##  nao e possivel econtrar o ano
       }
    } else if (count == 2) {
      if (getNidLength(new_nid) %in% c(15, 16, 17,18,19,20,21)) {   # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
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
getProxSequenciaNid <- function(nid,df.all.patients.openmrs){
  
  seq_actual <- getNumSeqNid(nid)
  
  if(seq_actual!=0){
    df <- df.all.patients.openmrs
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

#' Formata a Sequencia do NID 
#' 
#' @param NID do paciente
#' @return  usCode  
#' @examples formatUsCode(0111030701/2010/00195)
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
    
  } else{
   
      count <- str_count(new_nid, '/')  # Quantas barras tem o nid
      
      if (count == 1) {
        if (getNidLength(new_nid) %in% c(4, 5, 6, 7, 8)) {
          ## nids  4,5,6, 7,8)
          
          if(0 != getUsCode(nid)){ 
            if(0 != getAnoNid(nid)){
              nid_reformatado <- formatNidUmaBarra(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{ return(0) } }
        else {
          return(0) ## Nao foi possivel formatar o nid
        }
        
      } else if (count == 2) {
        if (getNidLength(new_nid) %in% c(15, 16, 17,18,19,20)) {       # ex 1100711/18/0729, 11030701/15/0023, 11030701/08/04676, 0110040701/19/0547
          
          if(0 != getUsCode(nid)){ 
            if(0 != getAnoNid(nid)){
              nid_reformatado <- formatNidDuasBarras(new_nid)
              return(nid_reformatado)
            } else{return(0)}
            
          } 
          else{return(0)}
        } 
        else if(getNidLength(new_nid)==21){ #  nao e possivel reformatar o nid com script deve ser manualemnte}
          return(new_nid)
        } else{ return(0) }
        
      } else {
        return(0) ##  nao e possivel reformatar o nid com script deve ser manualemnte}
        
      }
    } 
  
}

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
  } else {
    return(ano)
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
  } else {
    return(seq)
  }
  
}

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
  nid
  
  
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