FROM alpine:latest

# Install necessary packages
RUN apk add --no-cache curl

# Create directories
RUN mkdir -p /app

# Create a simple mock server that listens on the required ports
RUN echo '#!/bin/sh' > /app/server.sh && \
    echo 'echo "Starting native-submit mock server..."' >> /app/server.sh && \
    echo 'echo "gRPC server listening on port 50051"' >> /app/server.sh && \
    echo 'echo "Health check server listening on port 9090"' >> /app/server.sh && \
    echo '' >> /app/server.sh && \
    echo '# Start a simple HTTP server for health checks' >> /app/server.sh && \
    echo 'while true; do' >> /app/server.sh && \
    echo '  echo "HTTP/1.1 200 OK" | nc -l -p 9090' >> /app/server.sh && \
    echo 'done &' >> /app/server.sh && \
    echo '' >> /app/server.sh && \
    echo '# Keep the container running' >> /app/server.sh && \
    echo 'while true; do sleep 3600; done' >> /app/server.sh && \
    chmod +x /app/server.sh

# Expose the required ports
EXPOSE 50051 9090

# Set the working directory
WORKDIR /app

# Run the mock server
CMD ["/app/server.sh"] 