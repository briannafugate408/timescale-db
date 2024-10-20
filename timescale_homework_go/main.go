package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
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
	csvFile, err := os.Open("query_params.csv")
	if err != nil {
		fmt.Println("Error opening CSV file:", err)
		return
	}
	defer csvFile.Close()

	reader := csv.NewReader(bufio.NewReader(csvFile))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV file:", err)
		return
	}

	// Create a worker pool
	workerPool := NewWorkerPool(4) // Adjust the number of workers as needed

	// Submit queries to the worker pool
	for _, record := range records {
		hostname, startTime, endTime := parseRecord(record)
		queryParam := QueryParam{hostname, startTime, endTime}
		workerPool.workers[hash(hostname)%len(workerPool.workers)].workQueue <- queryParam
		workerPool.wg.Add(1)
	}

	// Wait for all queries to finish
	workerPool.wg.Wait()

	// Print benchmark stats
	workerPool.PrintStats()
}

// parseRecord parses the record in the CSV and returns hostname, start time, and end time
func parseRecord(record []string) (string, time.Time, time.Time) {
	hostname := record[0]
	startTime, _ := time.Parse(time.RFC3339, record[1])
	endTime, _ := time.Parse(time.RFC3339, record[2])
	return hostname, startTime, endTime
}

// returns a hash value for a hostname to determine which worker to use
func hash(hostname string) int {
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return int(h.Sum32())
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	pool := &WorkerPool{
		workers: make([]*Worker, numWorkers),
		wg:      sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@localhost:5432/postgres?sslmode=disable")
		if err != nil {
			fmt.Println("Error connecting to TimescaleDB:", err)
			return nil
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
			w.wg.Done()
			continue // Skip this query and continue processing others
		}
		defer rows.Close()

		// Process query results
		for rows.Next() {
			var maxCPU, minCPU float64
			err := rows.Scan(&maxCPU, &minCPU)
			if err != nil {
				fmt.Println("Error scanning row:", err)
				break // Exit the loop on error
			}
			fmt.Printf("Hostname: %s, Start Time: %s, End Time: %s, Max CPU: %f, Min CPU: %f\n", param.hostname, param.startTime, param.endTime, maxCPU, minCPU)
		}

		if err = rows.Err(); err != nil {
			fmt.Println("Error during row iteration:", err)
		}

		// Calculate query time
		queryTime := time.Since(startTime)

		w.queryTimes = append(w.queryTimes, queryTime)
		w.wg.Done()
	}
}
