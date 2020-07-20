library(tibble)
library(writexl)
# Verifica e actualiza pacientes que tem dados no iDART diferentes do OpenMRS
# --  cidalia joao
# ******** Configure para o dir onde deixou os ficheiros necessarios para executar o programa ****

wd <- '~/R/iDART/idart-scripts/dataClean/'

# Limpar o envinronment
rm(list=setdiff(ls(), c("wd", "tipo_nid")))

if (dir.exists(wd)){
  
  setwd(wd)  
  source('paramConfiguration.R')     
  
} else {
  
  message( paste0('O Directorio ', wd, ' nao existe, por favor configure corectamente o dir'))
}


postgres.user ='postgres'                       # ******** modificar
postgres.password='postgres'                    # ******** modificar
postgres.db.name='bagamoio'                        # ******** modificar
postgres.host='172.18.0.3'                   # ******** modificar
postgres.port=5432                              # ******** modificar

con_farmac_sync <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
con_bagamoio <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
patients_bagamoio <- dbGetQuery(con_bagamoio, "select * from patient ;")
###########################################################################


sync_patients <- dbGetQuery(con_farmac_sync, "select * from  sync_temp_patients where clinicname='Farmacia Jardim' ;")




patients_jardim <- dbGetQuery(con_postgres, "select * from  patient ;")


discordantes <- anti_join(patients_jardim,sync_patients , by="patientid")

discordantes$novo_nid_us <-""

for (v in 3:nrow(discordantes)) {
  
  # f_name <- discordantes$firstnames[v]
  # l_name <- discordantes$lastname[v]
  uuid_discordante <- discordantes$uuid[v]
  antigo_nid <-discordantes$patientid[v] 
  
  df_match <- subset(patients_bagamoio, patients_bagamoio$uuid==uuid_discordante , )
  
  if(nrow(df_match)==1){
    novo_nid <- df_match$patientid[1]
    
    discordantes$novo_nid_us[v] <-novo_nid
    if(is.na(novo_nid) | novo_nid =="")
    {
      print("Paciente do porto")
      
    } else {
      
      # dbExecute(con_postgres, paste0("update patientidentifier set value = '"  ,novo_nid , "' where value = '" ,antigo_nid ,"' ;"))
      # print(paste0("update patientidentifier set value = '"  ,novo_nid , "' where value = '" ,antigo_nid ,"' ;"))
      # dbExecute(con_postgres, paste0("update patient set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      # print(paste0("update patient set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      dbExecute(con_postgres, paste0("update sync_temp_patients set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      print( paste0("update sync_temp_patients set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      dbExecute(con_postgres, paste0("update packagedruginfotmp set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      print(paste0("update packagedruginfotmp set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      dbExecute(con_postgres, paste0("update sync_temp_dispense set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      print(paste0("update sync_temp_dispense set patientid = '"  ,novo_nid , "' where patientid = '" ,antigo_nid ,"' ;"))
      
    }

  }
}
