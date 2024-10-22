# Use the official Golang image as the builder
FROM golang:1.22 AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files to the working directory
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application code
COPY . .

# Create the bin directory
RUN mkdir -p bin


# Build the Go application
RUN go build -o bin/timescale_homework_go .

# Make the binary executable
RUN chmod +x bin/timescale_homework_go

# Use a minimal base image to run the application
FROM alpine:latest

# Install glibc compatibility for Go binaries
RUN apk add --no-cache libc6-compat

# Set the working directory in the final image
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/bin/timescale_homework_go /app/bin/timescale_homework_go

# Copy the CSV file into the container
COPY query_params.csv /app/query_params.csv
COPY cpu_usage.sql /app/cpu_usage.sql
COPY cpu_usage.csv /app/cpu_usage.csv

# Set environment variables (optional defaults)
ARG DB_HOST=timescaledb
ARG DB_PORT=5432
ARG DB_USER=postgres
ARG DB_PASSWORD=password
ARG DB_NAME=homework

ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_NAME=${DB_NAME}

# Expose the port your application listens on (adjust if necessary)
EXPOSE 5432

# Command to run the application
CMD ["/app/bin/timescale_homework_go"] 