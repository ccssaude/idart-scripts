

#' Busca o nome da US do openmrs
#' 
#' @param openmrs.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' default_loc = getOpenmrsDefaultLocation(con_openmrs)
getOpenmrsDefaultLocation <- function (openmrs.con){
  resut_set <- dbSendQuery(openmrs.con, "select property_value from global_property where property= 'default_location'")
  data <- fetch(resut_set,n=1)
  openmrs_default_location <- data$property_value[1]
  RMySQL::dbClearResult(resut_set)
  rm(data,resut_set)
  #detach("package:RMySQL", unload = TRUE)
  return (openmrs_default_location)
  
}

#' Busca o nome da US do Idart
#' 
#' @param postgres.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' clinic_name = getIdartClinicName(con_openmrs)
getIdartClinicName <- function (postgres.con){
    
  clinic_name <- dbGetQuery(postgres.con,sql_clinic_name)
  clinic_name <- clinic_name$clinicname[1]
  return(clinic_name)
}


#' Busca o nome da Farmacia no iDART
#' 
#' @param postgres.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' fac+name = getIdartFacilityName(con_openmrs)
getIdartFacilityName <- function (postgres.con){
  
  facility_name <- dbGetQuery(postgres.con,sql_facility_name)
  facility_name <- facility_name$facilityname[1]
  facility_name # same as  return(facility_name)

}



#' Busca o nome da US do Centro do Stock
#' 
#' @param postgres.con objecto de conexao com mysql    
#' @return us_default_locatoion
#' @examples
#' stock_name = getIdartStockCenterName(con_openmrs)
getIdartStockCenterName <- function (postgres.con){
  

  stockcenter_name <- dbGetQuery(postgres.con,"select stockcentername from public.stockcenter")
  stockcenter_name <- stockcenter_name$stockcentername[1]
  stockcenter_name
}



#' Busca o nos regimes existentes na tabela regimeterapeutico
#' 
#' @param postgres.con objecto de conexao com mysql    
#' @return regimeterapeutico
#' @examples
#' df_regimes = getIdartClinicName(con_openmrs)
getRegimesTerapeuticos <- function (postgres.con){

  reg_terap <- dbGetQuery(postgres.con,sql_regimes)
  reg_terap
}




#' Busca o  ultimo id (pk) de uma tabela 
#' 
#' @param postgres.con objecto de conexao com mysql   
#' @return ultimo id
#' @examples
#' res= getLastRegimeID(con_postgres)
getLastRegimeID <- function (postgres.con){
  
  df <- dbGetQuery(postgres.con, paste0('select regimeid from regimeterapeutico ;' ))
  last_id <- max(as.numeric(df$regimeid))
  last_id
  
}


#' Actualiza codRegime e  regimenopespecificado dum regimeter  na tabela regimeterapetico 
#' 
#' @param postgres.con objecto de conexao com mysql   
#' @param regimeid id do regime a ser actualizado na tabela regimeterapetico
#' @param cod.regime cod do regime por actualizar
#' @param regimenopespecificado  uuid  do regime no openmrs  
#' @return 1
#' 
#' @examples
#' res = actualizaRegimeTerapeutico(con_postgresregimeid,cod.regime,regimenopespecificado)
actualizaRegimeTerapeutico <- function(postges.con,regime.id,cod.regime,regimenopespecificado){
 
   dbExecute(postges.con,
            paste0(
              "update public.regimeterapeutico set regimenomeespecificado ='",
              regimenopespecificado,
              "'",
              " , codigoregime = '",
              cod.regime,
              "' where regimeid = ",
              regime.id
            )
  )
}


#' Insere codRegime e  regimenopespecificado dum regimeter  na tabela regimeterapetico 
#' 
#' @param postgres.con objecto de conexao com mysql   
#' @param  regimeid id do regime a ser actualizado na tabela regimeterapetico
#' @param  cod.regime cod do regime por actualizar
#' @param  regimenopespecificado  uuid  do regime no openmrs  
#' @return 1
#' 
#' @examples
#' res = insertRegimeTerapeutico(con_postgresregimeid,cod.regime,regimenopespecificado)
insertRegimeTerapeutico <- function(postges.con, regime.id ,regime.esquema,active, cod.regime, regimenopespecificado){
  
  dbExecute(
    postges.con,
    paste0(
      "INSERT INTO public.regimeterapeutico( regimeid, regimeesquema, active, codigoregime, regimenomeespecificado) VALUES (",
      regime.id,
      ", '",
      regime.esquema,
      "' ,",
      " ",
      active,
      " , '",
      cod.regime,
      "' , '",
      regimenopespecificado,
      "');"
    )
  )
  
}




#' Busca o ultimo id na tabela doctor 
#' 
#' @param postgres.con objecto de conexao com mysql   
#' @return regimeid id do regime a ser actualizado na tabela regimeterapetico
#' @return cod.regime cod do regime por actualizar
#' @return regimenopespecificado  uuid  do regime no openmrs  
#' @return 1
#' 
#' @examples
#' id = getLasTDoctorID(con_postgres)
getLastDoctorID <- function(postges.con){
  
  df <- dbGetQuery(postges.con, paste0('select id from doctor ;' ))
  last_id <- max(as.numeric(df$id))
  last_id
  
}