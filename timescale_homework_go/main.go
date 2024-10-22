package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

// WorkerPool represents a pool of workers for executing queries
type WorkerPool struct {
	workers []*Worker
	wg      sync.WaitGroup
}

// Worker represents a single worker in the pool
type Worker struct {
	id         int
	conn       *pgx.Conn
	workQueue  chan QueryParam
	queryTimes []time.Duration
	wg         *sync.WaitGroup
}

// QueryParam represents a param object
type QueryParam struct {
	hostname  string
	startTime time.Time
	endTime   time.Time
}

func main() {
	// Get the number of workers from user
	num := getNumWorkers()

	// Create and write to the homework database if it doesn't exist
	getOrCreateHomeworkDatabase()

	// Write data to the cpu_usage table
	writeData()

	// Read query parameters from CSV file and submit to worker pool
	csvFile, err := os.Open("query_params.csv")
	if err != nil {
		fmt.Println("Error opening CSV file:", err)
		return
	}

	reader := csv.NewReader(bufio.NewReader(csvFile))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV file:", err)
		return
	}

	defer csvFile.Close()

	// Create a worker pool
	workerPool := CreateWorkerPool(num) // Adjust the number of workers as needed

	// Submit queries to the worker pool
	for _, record := range records {
		hostname, startTime, endTime := parseRecord(record)
		queryParam := QueryParam{hostname, startTime, endTime}
		workerPool.workers[hash(hostname)%len(workerPool.workers)].workQueue <- queryParam
		workerPool.wg.Add(1)
	}

	// Close all work queues
	for _, worker := range workerPool.workers {
		close(worker.workQueue)
	}

	// Wait for all queries to finish
	workerPool.wg.Wait()

	// Print benchmark stats
	workerPool.PrintStats()
}

func writeData() {
	// Connect to TimescaleDB
	conn := connectToTimescaleDB()
	if conn == nil {
		fmt.Println("Error connecting to TimescaleDB")
		return
	}
	defer conn.Close(context.Background())

	// Check if cpu_usage table exists
	var exists bool
	err := conn.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'cpu_usage');").Scan(&exists)
	if err != nil {
		log.Fatalf("Error checking if table exists: %v\n", err)
	}

	if !exists {
		// Create the cpu_usage table
		if err := createCPUUsageTable(conn); err != nil {
			log.Fatalf("Failed to create cpu_usage table: %v\n", err)
		}
	} else {
		fmt.Println("Table cpu_usage already exists")
	}

	// Write data to the cpu_usage table
	if err := writeDataToCPUsageTable(conn); err != nil {
		log.Fatalf("Failed to write data to cpu_usage table: %v\n", err)
	}
}

func createCPUUsageTable(conn *pgx.Conn) error {
	sqlFile, err := os.ReadFile("cpu_usage.sql")
	if err != nil {
		return fmt.Errorf("error reading SQL file: %v", err)
	}

	_, err = conn.Exec(context.Background(), string(sqlFile))
	if err != nil {
		return fmt.Errorf("error executing SQL script: %v", err)
	}

	fmt.Println("Successfully created cpu_usage table!")
	return nil
}

func writeDataToCPUsageTable(conn *pgx.Conn) error {
	dataFile, err := os.Open("cpu_usage.csv")
	if err != nil {
		return fmt.Errorf("error reading data file: %v", err)
	}
	defer dataFile.Close()

	// Create a new CSV reader
	reader := csv.NewReader(dataFile)

	// Read the header
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}

	// Prepare the copy from source
	var rows [][]interface{}
	for {
		record, err := reader.Read()
		if err == csv.ErrFieldCount {
			return fmt.Errorf("error reading CSV record: %v", err)
		}
		if err != nil {
			break
		}

		// Parse the timestamp
		ts, err := time.Parse("2006-01-02 15:04:05", record[0])
		if err != nil {
			return fmt.Errorf("error parsing timestamp: %v", err)
		}

		// Parse the usage value
		usage, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			return fmt.Errorf("error parsing usage value: %v", err)
		}

		rows = append(rows, []interface{}{ts, record[1], usage})
	}

	// Execute the copy from command
	_, err = conn.CopyFrom(
		context.Background(),
		pgx.Identifier{"cpu_usage"},
		[]string{"ts", "host", "usage"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("error executing COPY FROM command: %v", err)
	}

	fmt.Println("Successfully wrote data to cpu_usage table!")
	return nil
}

func getOrCreateHomeworkDatabase() {
	// Connect to the default database
	conn := connectToTimescaleDB()
	if conn == nil {
		fmt.Println("Error connecting to TimescaleDB")
		return
	}
	defer conn.Close(context.Background())

	var exists bool
	err := conn.QueryRow(context.Background(), "SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = 'homework');").Scan(&exists)
	if err != nil {
		log.Fatalf("Error checking if database exists: %v\n", err)
	}

	if !exists {
		// Create the homework database
		_, err = conn.Exec(context.Background(), "CREATE DATABASE homework;")
		if err != nil {
			log.Fatalf("Error creating database: %v\n", err)
		}
		fmt.Println("Successfully created homework database")
	} else {
		fmt.Println("Database homework already exists")
	}
}

// Get user input for the number of workers
func getNumWorkers() int {
	userInput := bufio.NewReader(os.Stdin)
	fmt.Println("Enter the number of workers: ")
	numWorkers, err := userInput.ReadString('\n')

	if err != nil {
		fmt.Println("Defaulting to 4 workers")
		numWorkers = "5"
	}

	num := int(numWorkers[0] - '0')
	return num
}

// Connects to the TimescaleDB instance
func connectToTimescaleDB() *pgx.Conn {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s?sslmode=disable",
		os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"))

	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		fmt.Println("Error connecting to TimescaleDB:", err)
		return nil
	}

	err = conn.Ping(context.Background())
	if err != nil {
		fmt.Println("Error pinging TimescaleDB:", err)
		conn.Close(context.Background())
		return nil
	}

	return conn
}

// Parse record parses the record in the CSV and returns hostname, start time, and end time
func parseRecord(record []string) (string, time.Time, time.Time) {
	hostname := record[0]
	startTime, _ := time.Parse(time.RFC3339, record[1])
	endTime, _ := time.Parse(time.RFC3339, record[2])
	return hostname, startTime, endTime
}

// Returns a hash value for a hostname to determine which worker to use
func hash(hostname string) int {
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return int(h.Sum32())
}

func CreateWorkerPool(numWorkers int) *WorkerPool {
	pool := &WorkerPool{
		workers: make([]*Worker, numWorkers),
		wg:      sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		// Each worker gets its own connection
		conn := connectToTimescaleDB()
		if conn == nil {
			log.Fatalf("Error connecting to TimescaleDB for worker %d\n", i)
		}

		pool.workers[i] = &Worker{
			id:        i,
			conn:      conn,
			workQueue: make(chan QueryParam),
			wg:        &pool.wg, // Pass the WaitGroup here
		}
		go pool.workers[i].Run()
	}
	return pool
}

// Prints the benchmark statistics
func (wp *WorkerPool) PrintStats() {
	var totalTime, minTime, maxTime time.Duration
	var queryTimes []time.Duration

	for _, worker := range wp.workers {
		for _, queryTime := range worker.queryTimes {
			totalTime += queryTime
			if len(queryTimes) == 0 || queryTime < minTime {
				minTime = queryTime
			}
			if queryTime > maxTime {
				maxTime = queryTime
			}
			queryTimes = append(queryTimes, queryTime)
		}
	}

	numQueries := len(queryTimes)
	avgTime := totalTime / time.Duration(numQueries)

	// Calculate median query time
	sort.Slice(queryTimes, func(i, j int) bool { return queryTimes[i] < queryTimes[j] })
	medianIndex := numQueries / 2
	var medianTime time.Duration
	if numQueries%2 == 0 {
		medianTime = (queryTimes[medianIndex-1] + queryTimes[medianIndex]) / 2
	} else {
		medianTime = queryTimes[medianIndex]
	}

	fmt.Println("Benchmark Stats:")
	fmt.Printf("  Number of queries: %d\n", numQueries)
	fmt.Printf("  Total processing time: %s\n", totalTime)
	fmt.Printf("  Minimum query time: %s\n", minTime)
	fmt.Printf("  Median query time: %s\n", medianTime)
	fmt.Printf("  Average query time: %s\n", avgTime)
	fmt.Printf("  Maximum query time: %s\n", maxTime)
}

// Executes the worker's main loop
func (w *Worker) Run() {
	for param := range w.workQueue {
		startTime := time.Now()

		// Query to execute
		sql := `
            SELECT MAX(usage), MIN(usage)
            FROM cpu_usage
            WHERE host = $1 AND ts >= $2 AND ts < $3
            GROUP BY time_bucket('1 minute', ts)
        `

		// Execute the query with separate value arguments
		rows, err := w.conn.Query(context.Background(), sql, param.hostname, param.startTime, param.endTime)
		if err != nil {
			fmt.Println("Error executing query:", err)
			w.wg.Done() // Ensure Done() is called
			continue
		}

		// Process query results
		for rows.Next() {
			var maxCPU, minCPU float64
			if err := rows.Scan(&maxCPU, &minCPU); err != nil {
				fmt.Println("Error scanning row:", err)
				break // Exit the loop on error
			}
			fmt.Printf("Hostname: %s, Start Time: %s, End Time: %s, Max CPU: %f, Min CPU: %f\n", param.hostname, param.startTime, param.endTime, maxCPU, minCPU)
		}

		if err = rows.Err(); err != nil {
			fmt.Println("Error during row iteration:", err)
		}
		rows.Close()

		// Calculate query time
		queryTime := time.Since(startTime)
		w.queryTimes = append(w.queryTimes, queryTime)

		w.wg.Done() // Ensure Done() is always called here
	}
}
