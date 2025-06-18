## deleteDB.PractI.IlicF
## Filip Ilic
## Summer 1  2025

library(RMySQL)

db_host_aiven <- "" # your host goes here
db_port_aiven <- 0 # your port goes here
db_name_aiven <- "" # your name goes here
db_user_aiven <- "" # your user goes here
db_pwd_aiven <- "" # your pwd goes here

mydb.aiven <- dbConnect(RMySQL::MySQL(), 
                        user = db_user_aiven,
                        password = db_pwd_aiven,
                        dbname = db_name_aiven,
                        host = db_host_aiven,
                        port = db_port_aiven)

## Write all drop statements
drop_incidents <- "DROP TABLE IF EXISTS Incidents;"
drop_flights <- "DROP TABLE IF EXISTS Flights;"
drop_rb <- "DROP TABLE IF EXISTS ReportedBy;"
drop_it <- "DROP TABLE IF EXISTS IncidentTypes;"
drop_severity <- "DROP TABLE IF EXISTS Severity;"
drop_aircrafts <- "DROP TABLE IF EXISTS Aircrafts;"
drop_airports <- "DROP TABLE IF EXISTS Airports;"
drop_airlines <- "DROP TABLE IF EXISTS Airlines;"

# Execute all drop statements in a transaction to prevent partial screw ups
dbExecute(mydb.aiven, "START TRANSACTION;")
dbExecute(mydb.aiven, drop_incidents)
dbExecute(mydb.aiven, drop_flights)
dbExecute(mydb.aiven, drop_rb)
dbExecute(mydb.aiven, drop_it)
dbExecute(mydb.aiven, drop_severity)
dbExecute(mydb.aiven, drop_aircrafts)
dbExecute(mydb.aiven, drop_airports)
dbExecute(mydb.aiven, drop_airlines)
dbExecute(mydb.aiven, "COMMIT;")

dbDisconnect(mydb.aiven)

