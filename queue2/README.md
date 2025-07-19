# Description
#### Demo Redis Queue:
- producer: send messages to the task queue (files, str, dict, etc.)
- worker: receive messages from the queue and process them, send the result to result queue
  - retry on failure
  - 
  - queues:
    - job_queue
    - processing_queue
    - result_queue
- consumer: consume results from the result queue, print them, and save to file

# Installations
- python3.12
- redis
- pydantic
- loguru

# Usage
- Start Redis, worker.py, consumer.py
- Call producer.py to send messages to the queue
# Example
```bash
python worker.py
python consumer.py
python producer.py
```