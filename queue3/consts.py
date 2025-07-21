from enum import StrEnum

REDIS_MAX_RETRIES = 2  # total tries = first + 2 retries = 3 attempts

class RedisQueue(StrEnum):
    TASK = "task_q"
    TASK_310 = "task_310_q"
    TASK_RESULT = "task_res_q"
    TASK_PROCESSING = "task_processing_q"
    WORKER_HEALTH_CHECK = "worker_health_check"