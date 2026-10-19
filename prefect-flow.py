"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time

import boto3
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
import duckdb

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS

@task(retries=2, retry_delay_seconds=5)
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    resp = requests.post(SCATTER_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    logger.info("Scatter API answered: %s", payload)
    return payload["sqs_url"]

COUNTERS = ["ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"]

def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTERS)["Attributes"]
    return {"visible":   int(attrs["ApproximateNumberOfMessages"]),
            "in_flight": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            "delayed":   int(attrs["ApproximateNumberOfMessagesDelayed"])}


@task
def monitor_queue(queue_url: str, poll_s: int = 20, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        c = get_counts(queue_url)
        logger.info("visible=%d  in_flight=%d  delayed=%d",
                    c["visible"], c["in_flight"], c["delayed"])
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        time.sleep(poll_s)
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")


@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    stored = 0
    seen = set()
    deadline = time.monotonic() + timeout_s
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("""
    CREATE OR REPLACE TABLE raw.fragments (
        order_no VARCHAR,
        word VARCHAR,
        received_at TIMESTAMP
    )
    """)
    while stored < expected and time.monotonic() < deadline:
        resp = sqs.receive_message(
            QueueUrl = queue_url, 
            MaxNumberOfMessages = min(10, expected - stored),
            WaitTimeSeconds = 20,
            MessageAttributeNames = ["All"],
        )
        messages = resp.get("Messages", [])
        for message in messages:
            
            receipt = message["ReceiptHandle"]
            if "word" not in message["MessageAttributes"]:
                sqs.delete_message(
                        QueueUrl = queue_url, 
                        ReceiptHandle = receipt)
                logger.log("Message without word deleted")
                continue
            order_no = message["MessageAttributes"]["order_no"]["StringValue"] 
            if order_no in seen:
                sqs.delete_message(
                    QueueUrl = queue_url, 
                    ReceiptHandle = receipt)
                logger.log("Repeat message received and deleted")
                continue
            word = message["MessageAttributes"]["word"]["StringValue"]
            received_at = time.time()
            conn.execute("""
                INSERT into raw.fragments (order_no, word, received_at) 
                VALUES (?, ?, ?)
            """, (order_no, word, received_at))
            sqs.delete_message(
                QueueUrl = queue_url, 
                ReceiptHandle = receipt)
            seen.add(order_no)
            stored += 1
            logger.info(f"Stored {stored}/{expected} fragments")
    if stored < expected:
        raise TimeoutError(f"Failure: {expected} Messages not stored after {timeout_s} s")
    c = get_counts(queue_url)
    logger.info("visible=%d  in_flight=%d  delayed=%d",
                c["visible"], c["in_flight"], c["delayed"])
    if c["delayed"] == 0 and stored == expected:
                logger.info("All messages stored")
    conn.close()
    return stored

@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    raise NotImplementedError


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)
    collect_messages(queue_url)
    # TODO (Lesson 10): collect_messages -> dbt_build -> read_phrase -> submit_solution


if __name__ == "__main__":
    dp2_pipeline()
