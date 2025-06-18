## createDB.PractI.LastNameF.R
## Filip Ilic
## Summer 1 2025

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

# Create tables
create_airlines <- "
CREATE TABLE IF NOT EXISTS Airlines (
airline_id INT PRIMARY KEY AUTO_INCREMENT,
airline CHAR(2) NOT NULL UNIQUE
);"

create_airports <- "
CREATE TABLE IF NOT EXISTS Airports (
airport_id INT PRIMARY KEY AUTO_INCREMENT,
dep_airport CHAR(3) NOT NULL UNIQUE
);"

create_aircrafts <- "
CREATE TABLE IF NOT EXISTS Aircrafts (
aircraft_id INT PRIMARY KEY AUTO_INCREMENT,
aircraft_family VARCHAR(20) NOT NULL,
aircraft_model VARCHAR(20) NOT NULL,
UNIQUE (aircraft_family, aircraft_model)
);"

create_severity <- "
CREATE TABLE IF NOT EXISTS Severity (
sid INT PRIMARY KEY AUTO_INCREMENT,
severity VARCHAR(20) NOT NULL UNIQUE
);"

create_incident_types <- "
CREATE TABLE IF NOT EXISTS IncidentTypes (
itid INT PRIMARY KEY AUTO_INCREMENT,
incident_type VARCHAR(20) NOT NULL UNIQUE
);"

create_reported_by <- "
CREATE TABLE IF NOT EXISTS ReportedBy (
rbid INT PRIMARY KEY AUTO_INCREMENT,
reported_by VARCHAR(20) NOT NULL UNIQUE
);"

create_flights <- "
CREATE TABLE IF NOT EXISTS Flights (
fid INT PRIMARY KEY AUTO_INCREMENT,
flight_number INT NOT NULL,
delay_mins INT DEFAULT 0,
airline_id INT NOT NULL,
aircraft_id INT NOT NULL,
airport_id INT NOT NULL,
FOREIGN KEY (airline_id) REFERENCES Airlines(airline_id),
FOREIGN KEY (aircraft_id) REFERENCES Aircrafts(aircraft_id),
FOREIGN KEY (airport_id) REFERENCES Airports(airport_id),
UNIQUE (flight_number, delay_mins, airline_id, aircraft_id, airport_id)
);"

create_incidents <- "
CREATE TABLE IF NOT EXISTS Incidents (
iid INT PRIMARY KEY AUTO_INCREMENT,
csv_iid VARCHAR(10) NOT NULL UNIQUE,
date DATE NOT NULL,
num_injuries INT DEFAULT 0,
itid INT NOT NULL,
sid INT NOT NULL,
rbid INT NOT NULL,
fid INT NOT NULL,
FOREIGN KEY (itid) REFERENCES IncidentTypes(itid),
FOREIGN KEY (sid) REFERENCES Severity(sid),
FOREIGN KEY (rbid) REFERENCES ReportedBy(rbid),
FOREIGN KEY (fid) REFERENCES Flights(fid)
);"

# Push tables to DB in transaction to prevent partial screw ups
dbExecute(mydb.aiven, "START TRANSACTION;")
dbExecute(mydb.aiven, create_airlines)
dbExecute(mydb.aiven, create_airports)
dbExecute(mydb.aiven, create_aircrafts)
dbExecute(mydb.aiven, create_severity)
dbExecute(mydb.aiven, create_incident_types)
dbExecute(mydb.aiven, create_reported_by)
dbExecute(mydb.aiven, create_flights)
dbExecute(mydb.aiven, create_incidents)
dbExecute(mydb.aiven, "COMMIT;")

dbDisconnect(mydb.aiven)

