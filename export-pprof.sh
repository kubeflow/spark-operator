#!/bin/bash

# Generate a timestamp
timestamp=$(date +%Y-%m-%d_%H-%M-%S)

# Function to fetch goroutine profile
fetch_goroutine_profile() {
    local profile_type=$1  # 'start' or 'end'
    local profile_path="/tmp/goroutine-profile-debug1-$profile_type-$timestamp.prof"
    echo "Starting to fetch Goroutine profile ($profile_type) at $(date)..."
    curl "http://localhost:6060/debug/pprof/goroutine?debug=1" -o "$profile_path"
    echo "Completed fetching Goroutine profile ($profile_type) at $(date). File saved to $profile_path"
}

echo "Starting to fetch CPU profile at $(date)..."
cpu_profile="/tmp/cpu-profile-30sec-$timestamp.prof"
curl "http://localhost:6060/debug/pprof/profile?seconds=30" -o "$cpu_profile" &
echo "CPU profile fetch initiated, running for 30 seconds..."

echo "Starting to fetch Trace profile at $(date)..."
trace_profile="/tmp/trace-profile-30sec-$timestamp.prof"
curl "http://localhost:6060/debug/pprof/trace?seconds=30" -o "$trace_profile" &
echo "Trace profile fetch initiated, running for 30 seconds..."

echo "Fetching initial Goroutine profile..."
fetch_goroutine_profile "start" &

# Wait for CPU and trace profiling to complete
wait

echo "Starting to fetch final Goroutine profile after waiting for other profiles to complete..."
fetch_goroutine_profile "end"

echo "All profiling data collected"

# Copying profiles to S3 bucket
echo "CPU profile output - $cpu_profile"
echo "Trace profile output - $trace_profile"
echo "Initial Goroutine profile output - /tmp/goroutine-profile-debug1-start-$timestamp.prof"
echo "Final Goroutine profile output - /tmp/goroutine-profile-debug1-end-$timestamp.prof"

