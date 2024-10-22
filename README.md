# Query Tool

## Overview
This Go application connects to a TimescaleDB instance, creates a database and a table for CPU usage data if they don't exist, and executes queries based on parameters read from a CSV file. It uses a worker pool to handle multiple queries concurrently, providing benchmarking statistics for the execution time of each query.

## Features
Creates a database and a cpu_usage table in TimescaleDB if they do not exist.
Reads query parameters (hostname, start time, end time) from a CSV file.
Utilizes a worker pool to execute queries concurrently.
Outputs benchmarking statistics, including total processing time, minimum, median, average, and maximum query times.

## Prerequisites
- Go (1.22 or later)
- Docker (for running TimescaleDB)
- A TimescaleDB instance running (local or remote)

## Setup Instructions
1. Clone the Repository:

   ` git clone https://github.com/briannafugate408/timescale-db.git`

3. Navigate to `timescale_homework_go` in your local instance of the repository

4. Build the Application:

   `go build -o bin/timescale_homework_go`

5. Make sure Docker Daemon is running

6. Build Docker container (feel free to use these variables below or edit your own):

    ```
      docker build --build-arg DB_HOST=timescaledb \
             --build-arg DB_PORT=5432 \
             --build-arg DB_USER=postgres \
             --build-arg DB_PASSWORD=password \
             --build-arg DB_NAME=homework \
             -t timescale_homework_go .

7. Run Docker container for timescaleDB:

      `docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg16`
8. Link app to timescaleDB container:

   `docker run -it --name app --link timescaledb:timescaledb timescale_homework_go`
   
9. Enter the number of workers you would like to use (e.g. 4)

## Application Logic
1. Database Connection: The application connects to the TimescaleDB instance using environment variables.
2. Table Creation: It checks for the existence of the cpu_usage table and creates it if not found.
3. Data Insertion: Reads the cpu_usage.csv file and inserts data into the cpu_usage table.
4. Query Execution: Reads query parameters from query_params.csv and submits queries to the worker pool.
5. Results Output: Prints results and statistics of the executed queries.

## Benchmarking Stats
After executing all queries, the application outputs the following statistics:
- Number of queries
- Total processing time
- Minimum query time
- Median query time
- Average query time
- Maximum query time

## Things to improve on: 
- A better more efficient way to insert data into the DB (i.e. batching)
- Connection pooling to handle my database connections more efficiently
- Limiting the rate of the incoming queries so that the DB doesn't get overloaded
