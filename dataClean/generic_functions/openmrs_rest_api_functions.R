

#' checkPatientUuidExistsOpenMRS ->  verifica se existe um paciente no openmrs com um det. uuidopenmrs
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples 
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 

checkPatientUuidExistsOpenMRS <- function(jdbc.properties, patient) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  status <- TRUE
  url.check.patient <- paste0(base.url,'person/',patient[4])
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  if("error" %in% names(r)){
    message("Object with given uuid doesn't exist" )
    return(FALSE)
    } else{
    
    return(status)
    
  }
  

}


#' ReadJdbcProperties ->  carrega os paramentros de conexao no ficheiro jdbc.properties
#' @param file  patg to file
#' @return  vec [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @examples 
#' user_admin  <- ReadJdbcProperties(file)
#' 

readJdbcProperties <- function(file='jdbc.properties') {
  
  
  vec <- as.data.frame(read.properties(file = file ))
  vec
}



#' apiGetPatientByNid ->  verifica se existe um paciente no openmrs com um det. nid
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples  /openmrs/ws/rest/v1/patient?q=
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 

apiGetPatientByNid <- function(jdbc.properties, patientid ) {
  
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(base.url,'patient?q=' ,patientid[2])
  openmrsuuid <- ""
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  return(r)
  
}

#' apiGetPatientByName ->  verifica se existe um paciente no openmrs com um det. nome
#' @param jdbc.properties  [urlBase,urlBaseReportingRest,location,hibernateOpenMRSConnectionUrl,hibernateOpenMRSPassword,hibernateOpenMRSUsername]
#' @return  TRUE/FALSE 
#' @examples  /openmrs/ws/rest/v1/patient?q=
#' status  <- checkPatientUuidExistsOpenMRS(jdbc.properties, c("0102010001/2006/04892","julia" ,"jaoquina", "cb90174b-81e9-43e4-9b4d-dc09d2966efa"))
#' 
apiGetPatientByName <- function(jdbc.properties, patient.full.name ) {
  # url da API
  base.url.rest <- as.character(jdbc.properties$urlBaseReportingRest)
  base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(base.url,'patient?q=' ,gsub(pattern = ' ', replacement = '%20' ,x =patient.full.name,ignore.case = TRUE ))
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}




apiCheckEstadoPermanencia <- function(jdbc.properties, patient.uuid ) {
  # url da API
  url.base.reporting.rest<- as.character(jdbc.properties$urlBaseReportingRest)
  #base.url <-  as.character(jdbc.properties$urlBase)
  url.check.patient <- paste0(url.base.reporting.rest,'?personUuid=' ,patient.uuid)
  
  r <- content(GET(url.check.patient, authenticate('admin', 'eSaude123')), as = "parsed")
  
  return(r)
}

