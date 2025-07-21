# Description
#### Demo Redis Queue:
- Similar to redis queue2, BUT, queue3 has Flask API to send messages to the queue.
- Flask API Endpoints:
  - `GET /health`: server status
  - `GET /api/workers/health`: workers status
  - `GET /api/results`: get all results
  - `GET /api/results/<task_id>'`: get result by task_id
  - `POST /api/push`: push message to the queue
    - Headers: 
      - `Content-Type: application/json`
    - Request Body: 
      ```json
      {
        "num": 5
      }
      ```

# Installations
- python3.12
- install requirements.txt

# Usage
- Start Redis, Flask api, workers
```bash
docker-compose -f docker-compose.server.yml up --build -d 
```
```bash
docker-compose -f docker-compose.workers.yml up --build -d
```