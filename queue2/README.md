# Description
#### Demo Redis Queue:
- producer: send messages to the task queue (files, str, dict, etc.)
- worker: receive messages from the queue and process them, send the result to result queue
  - queues:
    - task_queue
    - task_processing_queue
    - task_result_queue
  - retry on failure
  - reaper: clean up old tasks in the task queue (only need 1 reaper)
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