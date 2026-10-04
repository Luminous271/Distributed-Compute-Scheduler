from job_queue.redis_queue import RedisQueue
from datetime import datetime, timezone
from scheduler.models import JobStatus
import threading
import time
from scheduler.retry_manager import RetryManager
from worker.execution_controller import execute_with_timeout

from scheduler.database import SessionLocal
from scheduler.db_models import JobDB as DBJob

# worker continuously asks redis, is there work? if so process job or no wait.
class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        # RedisQueue object allows a worker to communicate with redis 
        self.queue = RedisQueue()

        # create a consumer group if it does not already exist 
        self.queue.create_consumer_group()
        # register worker
        self.queue.register_worker(worker_id)

        # using python threading, we can  start the heart beat loop
        # remember that threads are I/O bound and job procesesing are I/O bound too. 
        # heartbeat thread can tick reliably even when the main 
        # worker thread is stalled with something (decoupled from logic)
        heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop,
            daemon=True) # marking the thread as a daemon means it wont prevent the proces from exiting
    
        # start the thread     
        heartbeat_thread.start()

        # the retry manager managees retrying jobs that failed or timed-out
        self.retry_manager = RetryManager(self.queue)

    # the main loop
    def run(self):
        print(f"Worker {self.worker_id} started")
        
        # continue until terminated
        while True:
            # use worker ids so redis knows which worker recieved what.

            # recover any jobs that have been idle
            self.recover_jobs()
            # get any new messages
            messages = self.queue.consume(consumer_name=self.worker_id)
            if not messages:
                continue
            # process the jobs
            for stream_name, entries in messages:
                for message_id, data in entries:
                    self.process_job(message_id, data)

    def process_job(self, message_id: str, data: dict):
        print(data)
        job_id = data["job_id"]
        job = self.queue.get_job(job_id)

        # check if the job exsists still
        if job is None:
            print(f"[{self.worker_id}] Job {job_id} not found")
            return

        # job is marked as running, set status and start time and this worker
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(timezone.utc)
        job.worker_id = self.worker_id
        job.message_id = message_id
        self.queue.save_job(job)

        print( f"[{self.worker_id}] " f"Processing job {job.id}")

        try:
            # Execute job
            result = execute_with_timeout(
                job.type,
                job.payload,
                job.timeout_seconds,
            )

            # current job is a copy of the job that was executed, between the time
            # job and the moment exectute_with_timeout happens, real time passes.
            # This could be longer than the job's timeout seconds. During that time
            # we should keep track of the status of the job in the redis job store.
            # just a safety check as something outside of this function might change
            # the redis queue

            # In short: current_job exists to re-validate ownership/state before committing results, 
            # protecting against race conditions 
            # from concurrent workers or timeout/reassignment logic that 
            # could have altered the job while it was executing.
            current_job = self.queue.get_job(job.id)
            if current_job is None:
                return
            if current_job.status != JobStatus.RUNNING:
                print(
                    f"[{self.worker_id}] "
                    f"Job {job.id} is no longer running. "
                    f"Ignoring result."
                )
                return

            # Mark job as COMPLETED
            current_job.status = JobStatus.COMPLETED
            current_job.completed_at = datetime.now(timezone.utc)
            current_job.result = result
            self.queue.save_job(current_job)
            self.queue.ack(message_id)

            print(
                f"[{self.worker_id}] "
                f"Completed job {job.id}: {result}"
            )
        except Exception as e:
            self.retry_manager.schedule_retry(
                job,
                message_id,
                e,
            )

    def recover_jobs(self) :
        messages = self.queue.claim_pending(
            consumer_name=self.worker_id,
            min_idle_ms=100_000,
            count=10,
        )
        for message_id, data in messages:
            print(
                f"[{self.worker_id}] "
                f"Recovered job {data['job_id']}"
            )
            self.process_job(message_id, data)
    # heartbeat loop, starts the thread that sends the worker monitor a heart beat
    def heartbeat_loop(self) :
        while True :
            self.queue.heartbeat_worker(self.worker_id)
            time.sleep(5)