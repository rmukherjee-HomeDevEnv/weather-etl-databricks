import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from dotenv import load_dotenv

load_dotenv()


def main():
    job_name = os.getenv("DATABRICKS_JOB_NAME", "weather_etl_daily")
    w = WorkspaceClient()

    job = next((j for j in w.jobs.list() if j.settings.name == job_name), None)
    if job is None:
        raise RuntimeError(f"No job named '{job_name}' found")

    run = w.jobs.run_now(job_id=job.job_id)
    print(f"Started run {run.run_id} for job '{job_name}'")

    deadline = time.time() + 900  # 15 minutes
    while True:
        status = w.jobs.get_run(run.run_id)
        state = status.state
        if state.life_cycle_state in (
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        ):
            break
        if time.time() > deadline:
            raise TimeoutError("Databricks job did not finish within 15 minutes")
        time.sleep(15)

    if state.result_state != RunResultState.SUCCESS:
        raise RuntimeError(f"Job run {run.run_id} finished as {state.result_state}")
    print(f"Job run {run.run_id} succeeded")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)