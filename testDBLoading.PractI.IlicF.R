## testDBLoading.PractI.IlicF
## Filip Ilic
## Summer 1 2025

library(RMySQL)

url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents.csv"
df <- read.csv(url, stringsAsFactors = FALSE)

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

## Check number of unique airlines
csv_airlines <- length(unique(df$airline))
sql_airlines <- dbGetQuery(mydb.aiven, "SELECT COUNT(*) AS count FROM Airlines;")
cat("Equal number of unique Airlines?: ", csv_airlines == sql_airlines$count)

## Check number of unique Flights
csv_flights <- nrow(unique(df[, c("flight.number", "delay.mins", "airline", "aircraft", "dep.airport")]))
sql_flights <- dbGetQuery(mydb.aiven, "SELECT COUNT(*) AS count FROM Flights;")
cat("Equal number of unique Flights?: ", csv_flights == sql_flights$count)

## Check number of unique Incidents
csv_incidents <- nrow(df)
sql_incidents <- dbGetQuery(mydb.aiven, "SELECT COUNT(*) AS count FROM Incidents;")
cat("Equal number of Incidents?: ", csv_incidents == sql_incidents$count)

## Check first and last dates
df$date <- as.Date(df$date, format = "%d.%m.%Y")
csv_first_date <- min(df$date)
csv_last_date <- max(df$date)
sql_dates <- dbGetQuery(mydb.aiven, "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM Incidents;")
cat("Same first date?: ", csv_first_date == sql_dates$min_date)
cat("Same last date?: ", csv_last_date == sql_dates$max_date)

## Compare average delay
csv_avg_delay <- mean(df$delay.mins)
sql_avg_delay <- dbGetQuery(mydb.aiven, "SELECT AVG(delay_mins) AS avg_delay FROM Flights;")
cat("Same average delay?:", round(csv_avg_delay, 2) == round(sql_avg_delay$avg_delay, 2))


## Compare number of injuries
csv_injuries <- sum(df$num.injuries)
sql_injuries <- dbGetQuery(mydb.aiven, "SELECT SUM(num_injuries) AS total_injuries FROM Incidents;")
cat("Same number of injuries?: ", csv_injuries == sql_injuries$total_injuries)

dbDisconnect(mydb.aiven)
