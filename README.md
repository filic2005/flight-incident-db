# flight-incident-db
R + MySQL project for designing and implementing a normalized relational database of bird strike incidents. Includes full ETL pipeline, schema creation, and SQL analytics.

## Overview

This project was built as part of a database systems course to practice designing, loading, and querying a real-world dataset in a relational database. The dataset includes flight delays, injuries, and incident types.

## Technologies

- **R** (with `RMySQL`)
- **MySQL** (hosted on Aiven)
- **RMarkdown**

## How It Works

1. **Load** CSV data directly from a remote source.
2. **Normalize** the data into 3NF with appropriate lookups and foreign keys.
3. **Create** all tables and constraints using R and SQL.
4. **Insert** lookup and fact data in batches for performance.
5. **Verify** the data integrity with sanity checks.
6. **Call** stored procedures to automate future insertions.

## Credentials

This repo contains **templated connection values**. Be sure to replace:
```r
db_user <- "your-username"
db_password <- "your-password"
```
with your own in local development. **No credentials are committed.**
