## loadDB.PractI.IlicF
## Filip Ilic
## Summer 1 2025

library(RMySQL)

url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents.csv"
df <- read.csv(url, stringsAsFactors = FALSE)
table(is.na(df$date))

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

## Clean data from CSV
## Start with Data Types

## Change date from chr to Date format
df$date <- as.Date(df$date, format = "%d.%m.%Y")
table(is.na(df$date))
df$delay.mins <- as.integer(df$delay.mins)
df$flight.number <- as.integer(df$flight.number)

## Split aircraft on the "-" in order to make aircraft_family and aircraft in the DB.
split_aircraft <- strsplit(df$aircraft, "-")
df$aircraft_family <- sapply(split_aircraft, "[", 1)
df$aircraft_model  <- sapply(split_aircraft, "[", 2)

## Insert Lookups first

## Insert Airlines
unique_airlines <- unique(df$airline)
airline_values <- paste0("('", unique_airlines, "')", collapse = ", ")
airlines_sql <- paste0("INSERT INTO Airlines (airline) VALUES ", airline_values, ";")

## Insert Airports
unique_airports <- unique(df$dep.airport)
airport_values <- paste0("('", unique_airports, "')", collapse = ", ")
airports_sql <- paste0("INSERT INTO Airports (dep_airport) VALUES ", airport_values, ";")

## Insert Aircrafts
unique_aircrafts <- unique(df[, c("aircraft_family", "aircraft_model")])
aircraft_values <- apply(unique_aircrafts, 1, function(row) {
  sprintf("('%s', '%s')", 
          row["aircraft_family"], 
          row["aircraft_model"])
})
aircraft_values <- paste(aircraft_values, collapse = ", ")
aircrafts_sql <- paste0("INSERT INTO Aircrafts (aircraft_family, aircraft_model) VALUES ", aircraft_values, ";")

## Insert Severity
unique_severities <- unique(df$severity)
severity_values <- paste0("('", unique_severities, "')", collapse = ", ")
severity_sql <- paste0("INSERT INTO Severity (severity) VALUES ", severity_values, "; ")

## Insert IncidentTypes
unique_it <- unique(df$incident.type)
## Not sure what 'faa' is, but it appears a lot and is used consistently among reporters, 
## so im going to assume it is not a typo.
incident_type_values <- paste0("('", unique_it, "')", collapse = ", ")
incident_type_sql <- paste0("INSERT INTO IncidentTypes (incident_type) VALUES ", incident_type_values, "; ")

## Insert ReportedBy
unique_rb <- unique(df$reported.by)
reportedby_values <- paste0("('", unique_rb, "')", collapse = ", ")
reportedby_sql <- paste0("INSERT INTO ReportedBy (reported_by) VALUES ", reportedby_values, "; ")

## Push Inserts
dbExecute(mydb.aiven, "START TRANSACTION;")
dbExecute(mydb.aiven, airlines_sql)
dbExecute(mydb.aiven, airports_sql)
dbExecute(mydb.aiven, aircrafts_sql)
dbExecute(mydb.aiven, severity_sql)
dbExecute(mydb.aiven, incident_type_sql)
dbExecute(mydb.aiven, reportedby_sql)
dbExecute(mydb.aiven, "COMMIT;")

## Insert Flights

#Get mappings for id to airline, airport, and aircraft
airline_lookup <- dbGetQuery(mydb.aiven, "SELECT airline_id, airline FROM Airlines;")
airport_lookup <- dbGetQuery(mydb.aiven, "SELECT airport_id, dep_airport FROM Airports;")
aircraft_lookup <- dbGetQuery(mydb.aiven, "SELECT aircraft_id, aircraft_family, aircraft_model FROM Aircrafts;")

# Merge airline IDs into df so that we can make a flight_df that has all the necessary columns as shown in the schema.
df <- merge(df, airline_lookup, 
            by.x = "airline", 
            by.y = "airline", 
            all.x = TRUE)
df <- merge(df, airport_lookup, 
            by.x = "dep.airport", 
            by.y = "dep_airport", 
            all.x = TRUE)
df <- merge(df, aircraft_lookup,
            by.x = c("aircraft_family", "aircraft_model"),
            by.y = c("aircraft_family", "aircraft_model"),
            all.x = TRUE)

## Create the flights DF so that the sql Inserts can just call on the values from these new columns.
flights_df <- unique(df[, c("flight.number", "airline_id", "aircraft_id", "airport_id", "delay.mins")])

flights_batch <- function() {
  total_rows <- nrow(flights_df)
  batches <- ceiling(total_rows / 500)
  
  for (i in 1:batches) {
    start <- ((i-1) * 500) + 1
    end <- min(start + 499, total_rows)
    batch <- flights_df[start:end, ]
    
    values <- apply(batch, 1, function(row) {
      sprintf("(%d, %d, %d, %d, %d)", 
              as.integer(row["flight.number"]), 
              as.integer(row["delay.mins"]), 
              as.integer(row["airline_id"]), 
              as.integer(row["aircraft_id"]), 
              as.integer(row["airport_id"]))
    })
    
    values <- paste(values, collapse = ", ")
    sql <- paste0("INSERT INTO Flights (flight_number, delay_mins, airline_id, aircraft_id, airport_id) VALUES ", values, ";")
    
    dbExecute(mydb.aiven, sql)
  }
}
flights_batch()

## Insert Incidents

#Get mappings for id to IT, severity, and RB
it_lookup <- dbGetQuery(mydb.aiven, "SELECT itid, incident_type FROM IncidentTypes;")
severity_lookup <- dbGetQuery(mydb.aiven, "SELECT sid, severity FROM Severity;")
rb_lookup <- dbGetQuery(mydb.aiven, "SELECT rbid, reported_by FROM ReportedBy;")
fid_lookup <- dbGetQuery(mydb.aiven, "SELECT fid, flight_number, delay_mins, airline_id, aircraft_id, airport_id FROM Flights;")
nrow(fid_lookup)

names(df)[names(df) == "num.injuries"] <- "num_injuries"
names(df)[names(df) == "reported.by"] <- "reported_by"
names(df)[names(df) == "incident.type"] <- "incident_type"
names(df)[names(df) == "iid"] <- "csv_iid"

str(df[, c("flight.number", "delay.mins", "airline_id", "aircraft_id", "airport_id")])
str(fid_lookup[, c("flight_number", "delay_mins", "airline_id", "aircraft_id", "airport_id")])
table(is.na(df$date))

df <- merge(df, it_lookup, 
            by.x = "incident_type", 
            by.y = "incident_type", 
            all.x = TRUE)
df <- merge(df, severity_lookup, 
            by.x = "severity", 
            by.y = "severity", 
            all.x = TRUE)
df <- merge(df, rb_lookup, 
            by.x = "reported_by", 
            by.y = "reported_by", 
            all.x = TRUE)
df <- merge(df, fid_lookup,
            by.x = c("flight.number", "delay.mins", "airline_id", "aircraft_id", "airport_id"),
            by.y = c("flight_number", "delay_mins", "airline_id", "aircraft_id", "airport_id"),
            all.x = TRUE)

## Create the flights DF so that the sql Inserts can just call on the values from these new columns.
incidents_df <- df[, c("csv_iid", "date", "num_injuries", "itid", "sid", "rbid", "fid")]
colSums(is.na(incidents_df))
nrow(incidents_df)
names(incidents_df)

incidents_batch <- function() {
  total_rows <- nrow(incidents_df)
  batches <- ceiling(total_rows / 500)
  
  for (i in 1:batches) {
    start <- ((i-1) * 500) + 1
    end <- min(start + 499, total_rows)
    batch <- incidents_df[start:end, ]
    
    values <- apply(batch, 1, function(row) {
      sprintf("('%s', '%s', %d, %d, %d, %d, %d)", 
              row["csv_iid"], 
              row["date"], 
              as.integer(row["num_injuries"]), 
              as.integer(row["itid"]), 
              as.integer(row["sid"]),
              as.integer(row["rbid"]),
              as.integer(row["fid"]))
    })
    
    values <- paste(values, collapse = ", ")
    sql <- paste0("INSERT INTO Incidents (csv_iid, date, num_injuries, itid, sid, rbid, fid) VALUES ", values, ";")
    
    dbExecute(mydb.aiven, sql)
  }
}
incidents_batch()

dbDisconnect(mydb.aiven)

