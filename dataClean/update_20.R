
setwd("~/R/iDART/")
source ("sql_querys.R")

library(RMySQL)

openmrs = dbConnect(MySQL(), user='esaude', password='esaude', dbname='openmrs', host='192.168.100.100')
rs <- dbSendQuery(openmrs, "select property_value from global_property where property= 'default_location'")
data <- fetch(rs,n=1)
new_clinic_name <- data$property_value[1]
rm(data,rs,openmrs)
detach("package:RMySQL", unload = TRUE)


library(RPostgreSQL)
#detach("package:RPostgreSQL", unload = TRUE)
con <-
  dbConnect(
    PostgreSQL(),
    user = "postgres",
    password = "postgres",
    dbname = "pharm",
    host = "192.168.100.50"
)


clinic_name <- dbGetQuery(con,sql_clinic_name)
clinic_name <- clinic_name$clinicname[1]
facility_name <- dbGetQuery(con,sql_facility_name)
facility_name <- facility_name$facilityname[1]
stockcenter_name <- dbGetQuery(con,"select stockcentername from public.stockcenter")
stockcenter_name <- stockcenter_name$stockcentername[1]
################################ Phase I - Cleaning duplicated patients


dups_nids <- dbGetQuery(con, sql_dups_nids)

vec <- ""
for (i in 1:dim(dups_nids)[1]) {
  if (i != dim(dups_nids)[1]) {
    vec <- paste0(vec, "'", dups_nids$patientid[i], "'", ",")
  } else {
    vec <- paste0(vec, "'", dups_nids$patientid[i], "'")
  }
}

dups_patient <-
  dbGetQuery(
    con,
    paste0(
      "select id, patientid, firstnames, lastname , uuidopenmrs from patient where patientid in (",
      vec,
      ")"
    )
  )
dups <- dups_patient[1, ]
dups$current
for (i in 1:dim(dups_nids)[1]) {
  temp <-
    dbGetQuery(
      con,
      paste0(
        "select id, patientid, firstnames, lastname , uuidopenmrs from patient where patientid=",
        "'",
        dups_nids$patientid[i],
        "'"
      )
    )
  dups <- rbind(dups, temp)
}

rm(dups_patient, temp)

################################ Phase II updating database structure

regimes <- dbGetQuery(con, sql_regimes)
regimes_antigos <- regimes
rm(regimes)

for (v in 1:length(sql_update_querys)) {
  dbExecute(con, sql_update_querys[v])
}

dbExecute(con, sql_create_tbl_query)

### Load Regimes
load(file = '~/R/iDART/regimes.RDATA')
regimes <- regimes[regimes$active == TRUE, ]
regimes <- regimes[order(regimes$regimeesquema), ]
regimes$adult[which(is.na(regimes$adult))] <- TRUE
regimes$regimeesquema[7]<-strtrim(regimes$regimeesquema[7],22)

regimes_antigos <- regimes_antigos[regimes_antigos$active == TRUE, ]

#regimes$status <- ""

for (i in 1:dim(regimes)[1]) {
  var_regime <- regimes$regimeesquema[i]
  var_control = TRUE
  for (v in 1:dim(regimes_antigos)[1]) {
    if (var_regime == regimes_antigos$regimeesquema[v])
      ## update
    {
      regimeid <- regimes_antigos$regimeid[v]
      dbExecute(
        con,
        paste0(
          "update public.regimeterapeutico set regimenomeespecificado ='",
          regimes$regimenomeespecificado[i],
          "'",
          " , codigoregime = ",
          regimes$codigoregime[i],
          " where regimeid = ",
          regimeid
        )
      )
      var_control = FALSE
  
    }
    
  }
  if (var_control) {
    dbExecute(
      con,
      paste0(
        "INSERT INTO public.regimeterapeutico( regimeid, regimeesquema, active, pediatrico, adult,  codigoregime, regimenomeespecificado) VALUES (",
        regimes$regimeid[i],
        ", '",
        regimes$regimeesquema[i],
        "' ,",
        " ",
        regimes$active[i],
        " , '",
        regimes$pediatrico[i],
        "' ,",
        " ",
        regimes$adult[i],
        " , ",
        regimes$codigoregime[i],
        " , '",
        regimes$regimenomeespecificado[i],
        "');"
      )
    )

    
    
  }
  
  
}

## Update facility name
dbExecute(
  con,
  paste0("  update public.nationalclinics set facilityname = '",new_clinic_name,"' where facilityname = '",clinic_name,"';"))

dbExecute(
  con,
  paste0("update public.clinic    set clinicname = '",new_clinic_name,"' where clinicname = '",facility_name,"';"))
    
dbExecute(
  con,
  paste0("update public.stockcenter    set stockcentername = '",new_clinic_name,"' where stockcentername = '",stockcenter_name,"';"))
   
dbExecute(con, sql_update_nid_pi)

dbExecute(con, "UPDATE public.regimeterapeutico SET  codigoregime=1  WHERE codigoregime is null;")





###  Misc code

update_nid <- dbGetQuery(con, "SELECT  pat.id, pat.patientid, nul.original_nid, new_nid, nul.uuid as nulluuuid,pat.uuidopenmrs, status
  FROM public.nid_update_log nul  inner join public.patient pat on nul.uuid=pat.patientid;
")

not_updated <- dbGetQuery(con, "SELECT  pat.id, pat.patientid, pat.uuidopenmrs, pat.firstnames, pat.lastname
  FROM public.patient pat where pat.uuidopenmrs is null")



for (i in 1:dim(update_nid)[1] ) {
  
  nid=update_nid$original_nid[i]
  patient_id=update_nid$id[i]
  
  if(nchar(nid)>17 ){
    dbExecute(con,paste0("update public.patientidentifier set value ='", nid,  "' where patient_id = ",  patient_id   ))
      
    
  }

}




wrong_size_nid <- dbGetQuery(con, "SELECT pat.id, pat.accountstatus,  pat.firstnames,   pat.lastname,pat.patientid,pat.uuid, pat.uuidopenmrs, ep.startreason
  FROM patient pat left join episode ep on pat.id=ep.patient 
  where uuidopenmrs is not null and (ep.startreason='' or ep.startreason='Novo Paciente'); ")

### excluir os transitos
wrong_size_nid <- subset(wrong_size_nid,!grepl(x=wrong_size_nid$patientid,pattern = "T") | 
                           wrong_size_nid$startreason=="" | wrong_size_nid$startreason=="Novo Paciente")

wrong_size_nid <- wrong_size_nid[which(nchar(wrong_size_nid$patientid)<21),]

nid_update_log <- dbGetQuery(con, "SELECT patient_id, nul.original_nid, nul.uuid ,nul.last_artpickup_date
  FROM public.nid_update_log nul  ;")

nid_update_log$name <- ""
nid_update_log$middle_name <- ""
nid_update_log$surname <- ""

for (i in 1:dim(nid_update_log)[1]) {
  
  patient_id <-nid_update_log$patient_id[i]
  pat <- dbSendQuery(openmrs, paste0("select pn.given_name,pn.middle_name,pn.family_name
					from person_name pn where pn.person_id = ",patient_id," ;") )
  data <- fetch(pat,n=1)
  nid_update_log$name[i] <- data$given_name[1]
  nid_update_log$middle_name[i] <- data$middle_name[1]
  nid_update_log$surname[i] <- data$family_name[1]
  RMySQL::dbClearResult(pat)
}


############ First:   match by name 
library(stringr)
library(plyr)
wrong_size_nid$new_nid <-""
wrong_size_nid$openmrs_name <-""
wrong_size_nid$openmrs_uuid <-""

for (v in 1:dim(wrong_size_nid)[1]) {
  
   first_name <- wrong_size_nid$firstnames[v]
   last_name <- wrong_size_nid$lastname[v]
  
  full <- str_split(first_name," ")
  f <- full[[1]][1]
  s <-full[[1]][2]
  
  patient <- subset(nid_update_log, ( nid_update_log$name==first_name & nid_update_log$surname==last_name )  | (nid_update_log$name==f & nid_update_log$middle_name==s & nid_update_log$surname==last_name) )
  if(!empty(patient)){
  wrong_size_nid$new_nid[v] <- patient$original_nid[1]
  if(is.na(patient$middle_name[1])| patient$middle_name[1]=="") {
    wrong_size_nid$openmrs_name[v] <- paste0(patient$name[1]," ",patient$surname[1])
  } else{ 
  wrong_size_nid$openmrs_name[v] <- paste0(patient$name[1]," ",patient$middle_name[1]," ",patient$surname[1]) }
  
  wrong_size_nid$openmrs_uuid[v] <- patient$uuid[1]
  
  
  }
  
}

matched_by_names <- as.data.frame(wrong_size_nid[which(wrong_size_nid$new_nid!=""),])
matched_by_names$matched <- ""
matched_by_names$pat_id <- ""
matched_by_names$obs <- "" 

### excluir os transitos
matched_by_names <- matched_by_names[which(grepl(x=matched_by_names$new_nid,pattern = "T")==FALSE),]

library(stringi)
## #########################  update the nids  
for (i in 1:dim(matched_by_names)[1]) {
  
  index <- stri_locate_last(matched_by_names$patientid[i], fixed='/')
  index <- index[[1]]
  if(!is.na(index)){
  seq <- substr(matched_by_names$patientid[i],index+1,nchar(matched_by_names$patientid[i]))
  
  if(grepl(pattern = seq,x = matched_by_names$new_nid[i])){
    matched_by_names$matched[i] <-TRUE
    # if(nchar(matched_by_names$new_nid[i])==21 && !is.na(matched_by_names$pat_id[i])){
    #   
    #   id <- matched_by_names$pat_id[i]
    #   dbExecute(con,paste0("update  public.patientidentifier set value ='", matched_by_names$new_nid[i],  "' where patient_id = ",  id,";"   ))
    #   dbExecute(con,paste0("update  public.patient set patientid ='", matched_by_names$new_nid[i],  "' where id = ",  id,";"   ))
    #   
    # } else {matched_by_names$obs[i]  <- "verificar  o tamanho do nid"}
  }
  }
}
matched_by_names <- matched_by_names[matched_by_names$matched==TRUE,]
# for (i in 8:dim(matched_by_names)[1]) {
# 
#         id <- matched_by_names$id[i]
#          dbExecute(con,paste0("update  public.patientidentifier set value ='", matched_by_names$new_nid[i],  "' where patient_id = ",  id,";"   ))
#          dbExecute(con,paste0("update  public.patient set patientid ='", matched_by_names$new_nid[i],  "' where id = ",  id,";"   ))
#          dbExecute(con,paste0("update  public.patient set uuid ='", matched_by_names$openmrs_uuid[i],  "' where id = ",  id,";"   ))
#          dbExecute(con,paste0("update  public.patient set uuidopenmrs ='", matched_by_names$openmrs_uuid[i],  "' where id = ",  id,";"   ))
#   
# }
################# get idart patient id

for (i in 1:dim(matched_by_names)[1]) {
  
  index <- matched_by_names$patientid[i]
  id <-   dbGetQuery(con,paste0("select id from   public.patient where patientid = '",  index,"'"   ))
  if(!empty(id)){
    matched_by_names$pat_id[i]  <- id$id[1]
  } else {    matched_by_names$obs[i]  <- "No match"}

}

matched_by_names$pat_id <- as.integer( matched_by_names$pat_id)

library(stringi)
## #########################  update the nids  
for (i in 316:dim(matched_by_names)[1]) {
  
  index <- stri_locate_last(matched_by_names$patientid[i], fixed='/')
  index <- index[[1]]
  seq <- substr(matched_by_names$patientid[i],index+1,nchar(matched_by_names$patientid[i]))
  
  if(grepl(pattern = seq,x = matched_by_names$new_nid[i])){
    matched_by_names$matched[i] <-TRUE
    if(nchar(matched_by_names$new_nid[i])==21 && !is.na(matched_by_names$pat_id[i])){
      
      id <- matched_by_names$pat_id[i]
      dbExecute(con,paste0("update  public.patientidentifier set value ='", matched_by_names$new_nid[i],  "' where patient_id = ",  id,";"   ))
     dbExecute(con,paste0("update  public.patient set patientid ='", matched_by_names$new_nid[i],  "' where id = ",  id,";"   ))
      
    } else {matched_by_names$obs[i]  <- "verificar  o tamanho do nid"}
  }
}


############################################  Second match by uuuid #


############ First:   match by uuuid 
library(stringr)
library(plyr)
wrong_size_nid$new_nid <-""
wrong_size_nid$openmrs_name <-""
wrong_size_nid$openmrs_uuid <-""

for (v in 1:dim(wrong_size_nid)[1]) {
  
  patientid <- wrong_size_nid$patientid[v]
  
  
  patient <- subset(nid_update_log,  nid_update_log$uuid==patientid,  )
  if(!empty(patient)){
    wrong_size_nid$new_nid[v] <- patient$original_nid[1]
    if(is.na(patient$middle_name[1])| patient$middle_name[1]=="") {
      wrong_size_nid$openmrs_name[v] <- paste0(patient$name[1]," ",patient$surname[1])
    } else{ 
      wrong_size_nid$openmrs_name[v] <- paste0(patient$name[1]," ",patient$middle_name[1]," ",patient$surname[1]) }
    
    wrong_size_nid$openmrs_uuid[v] <- patient$uuid[1]
  }
  
}

matched_by_names <- wrong_size_nid[which(wrong_size_nid$new_nid!=""),]

matched_by_names$matched <- ""
matched_by_names$pat_id <- ""
matched_by_names$obs <- "" 

################# get idart patient id

for (i in 1:dim(matched_by_names)[1]) {
  
  index <- matched_by_names$patientid[i]
  id <-   dbGetQuery(con,paste0("select id from   public.patient where patientid = '",  index,"'"   ))
  if(!empty(id)){
    matched_by_names$pat_id[i]  <- id$id[1]
  } else {    matched_by_names$obs[i]  <- "No match"}
  
}

matched_by_names$pat_id <- as.integer( matched_by_names$pat_id)


library(stringi)
## #########################  update the nids  
for (i in 1:dim(matched_by_names)[1]) {
  
  index <- stri_locate_last(matched_by_names$patientid[i], fixed='/')
  index <- index[[1]]
  seq <- substr(matched_by_names$patientid[i],index+1,nchar(matched_by_names$patientid[i]))
  
  if(grepl(pattern = seq,x = matched_by_names$new_nid[i])){
    matched_by_names$matched[i] <-TRUE
    if(nchar(matched_by_names$new_nid[i])==21 && !is.na(matched_by_names$pat_id[i])){
      
      id <- matched_by_names$pat_id[i]
      dbExecute(con,paste0("update  public.patientidentifier set value ='", matched_by_names$new_nid[i],  "' where patient_id = ",  id,";"   ))
      dbExecute(con,paste0("update  public.patient set patientid ='", matched_by_names$new_nid[i],  "' where id = ",  id,";"   ))
      
    } else {matched_by_names$obs[i]  <- "verificar  o tamanho do nid"}
  }
}



}