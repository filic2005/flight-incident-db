## configBusinessLogic.PractI.IlicF.R
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

## Q2
dbExecute(mydb.aiven, "DROP PROCEDURE IF EXISTS storeIncident;")
q2_sql <- " 
CREATE PROCEDURE storeIncident(
IN p_csv_iid VARCHAR(10),
IN p_date DATE,
IN p_num_injuries INT,
IN p_itid INT,
IN p_sid INT,
IN p_rbid INT,
IN p_fid INT)

BEGIN
INSERT INTO Incidents (csv_iid, date, num_injuries, itid, sid, rbid, fid)
VALUES (p_csv_iid, p_date, p_num_injuries, p_itid, p_sid, p_rbid, p_fid);
END;"
dbExecute(mydb.aiven, q2_sql)

## Now show that it works

q2_test <- "
CALL storeIncident(
'i19043',
'2025-06-16',
0,
1,
1,
1,
1);"
dbExecute(mydb.aiven, q2_test)

## Q3

dbExecute(mydb.aiven, "DROP PROCEDURE IF EXISTS storeNewIncident;")
q3_sql <- "
CREATE PROCEDURE storeNewIncident(
IN proc_csv_iid VARCHAR(10),
IN proc_date DATE,
IN proc_num_injuries INT,
IN proc_itid INT,
IN proc_sid INT,
IN proc_rbid INT,
IN proc_flight_number INT,
IN proc_delay_mins INT,
IN proc_airline CHAR(3),
IN proc_aircraft_family VARCHAR(20),
IN proc_aircraft_model VARCHAR(20),
IN proc_dep_airport CHAR(3))

BEGIN
DECLARE var_airline_id INT;
DECLARE var_aircraft_id INT;
DECLARE var_airport_id INT;
DECLARE var_fid INT;

INSERT IGNORE INTO Airlines (airline) 
VALUES (proc_airline);
SELECT airline_id INTO var_airline_id FROM Airlines 
WHERE airline = proc_airline;

INSERT IGNORE INTO Airports (dep_airport) 
VALUES (proc_dep_airport);
SELECT airport_id INTO var_airport_id FROM Airports 
WHERE dep_airport = proc_dep_airport;

INSERT IGNORE INTO Aircrafts (aircraft_family, aircraft_model) 
VALUES (proc_aircraft_family, proc_aircraft_model);
SELECT aircraft_id INTO var_aircraft_id FROM Aircrafts 
WHERE aircraft_family = proc_aircraft_family AND aircraft_model = proc_aircraft_model;

INSERT IGNORE INTO Flights (flight_number, delay_mins, airline_id, aircraft_id, airport_id)
VALUES (proc_flight_number, proc_delay_mins, var_airline_id, var_aircraft_id, var_airport_id);
SELECT fid INTO var_fid FROM Flights
WHERE flight_number = proc_flight_number AND delay_mins = proc_delay_mins AND airline_id = var_airline_id 
AND aircraft_id = var_aircraft_id AND airport_id = var_airport_id;

INSERT INTO Incidents (csv_iid, date, num_injuries, itid, sid, rbid, fid)
  VALUES (proc_csv_iid, proc_date, proc_num_injuries, proc_itid, proc_sid, proc_rbid, var_fid);
END;"
dbExecute(mydb.aiven, q3_sql)

# Test procedure
q3_test <- "
CALL storeNewIncident(
'i19044',
'2025-06-17',
0,
1,
1,
1,
1,
0,
'NH',
'737',
'800',
'LHR'
);"
dbExecute(mydb.aiven, q3_test)

dbDisconnect(mydb.aiven)
