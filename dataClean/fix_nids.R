patients <- dbGetQuery(con, "select * from patient")
pat_identifier <- dbGetQuery(con, "select * from patientidentifier")

pi_no_match <- dbGetQuery(con, "select pat.id, firstnames, lastname, patientid, uuidopenmrs, value, patient_id from patient pat
inner join patientidentifier  pi
on pat.patientid=pi.value")

nid_no_update <- subset(pi_no_match,nchar(pi_no_match$patientid) %in% c(15,16,17,18,22))

nid_no_update$given_name  <-  ""
nid_no_update$midle_name  <-  ""
nid_no_update$family_name <-  ""
nid_no_update$identifier <-  ""


library(RMySQL)

openmrs = dbConnect(MySQL(), user='esaude', password='esaude', dbname='openmrs', host='192.168.100.100')
for (i in 1:dim(nid_no_update)[1]){
  
  uuid <- nid_no_update$uuidopenmrs[i]
  temp <-NULL
  
  temp <- dbGetQuery(openmrs,paste0("SELECT  pi.identifier , pe.uuid, pn.given_name,pn.middle_name,pn.family_name
  FROM  patient pat INNER JOIN  openmrs.patient_identifier pi ON pat.patient_id =pi.patient_id
  INNER JOIN person pe ON pat.patient_id=pe.person_id
  INNER JOIN person_name pn ON pe.`person_id`=pn.person_id
WHERE pe.uuid ='",uuid,"';"))
  nid_no_update$given_name[i] <- temp$given_name[1]
  nid_no_update$midle_name[i]  <-  temp$middle_name[1]
  nid_no_update$family_name[i] <-  temp$family_name[1]
  nid_no_update$identifier[i]   <- temp$identifier[1]
  
  
}


to_update <- nid_no_update[which(nchar(nid_no_update$identifier)==21 && nid_no_update$lastname ==nid_no_update$family_name ),]
to_update_2 <- nid_no_update[which(nchar(nid_no_update$identifier)==21 & nid_no_update$lastname ==nid_no_update$family_name ),]
check  <- nid_no_update[which(nchar(nid_no_update$identifier)==21 & nid_no_update$lastname!=nid_no_update$family_name ),]

15, 44, 47, 182,253,269,333,411,435,445,683,1025,1068,1097,1124,1128,1134,1159,1164

0108010601/2010/00818
0108010601/2011/00006



for (i in 1165:dim(to_update_2)[1]){
  
  patientid <- to_update_2$patient_id[i]
  nid <- to_update_2$identifier[i]
  dbExecute(con, paste0(" update public.patientidentifier  set value = '",nid, "' where patient_id=",patientid ) )
  dbExecute(con, paste0(" update public.patient set patientid = '",nid ,"' ", " where id =",patientid  ) )
  
  }

