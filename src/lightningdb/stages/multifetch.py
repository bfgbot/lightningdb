import subprocess

from pydantic import BaseModel
from redis import Redis
from rq import Queue
from rq.command import send_kill_horse_command
from rq.job import Job
from rq.worker import Worker, WorkerStatus


class RQContext:
    def __init__(self):
        self.redis = Redis()
        self.queue = Queue(connection=self.redis, name="superfetch")

    # Send commands to all workers to abort their jobs
    def reset_all(self):
        workers = Worker.all(self.redis)
        for worker in workers:
            if worker.state == WorkerStatus.BUSY:
                send_kill_horse_command(self.redis, worker.name)


class MultiFetchError(Exception):
    pass


# MultiFetch uses Redis Queue (RQ) to distribute fetch tasks across multiple workers.
# RQContext sets up the Redis connection and queue.
# MultiFetch creates and manages the distributed jobs.
# Each job runs a subprocess to execute the 'superfetch' command for data fetching.
# Results are collected and processed with error handling in place.
class MultiFetch(BaseModel):
    def runall(self, input_pfiles: list[list[str]], output_dir: str) -> list[list[str]]:
        ctx = RQContext()

        jobs = []
        for input_files in input_pfiles:
            job = Job.create(
                subprocess.check_output,
                args=[
                    [
                        "/tmp/superfetch",  # the superfetch command is hardcoded
                        output_dir,
                        *input_files,
                    ]
                ],
                kwargs={"text": True},
                result_ttl="1d",
                timeout="5d",
                connection=ctx.redis,
            )
            ctx.queue.enqueue_job(job)
            jobs.append(job)

        output_pfiles = []
        for part, job in enumerate(jobs):
            while True:
                result = job.latest_result(timeout=10)
                if result is not None:
                    break
            if result.exc_string is not None:
                raise MultiFetchError(result.exc_string)
            if result.return_value is None:
                raise MultiFetchError(f"No result for part {part}")

            output_files = result.return_value.splitlines()
            output_pfiles.append(output_files)
        return output_pfiles
