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
get_counts and monitor_queue. 
Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. zThe tasks not called by the flow yet cannot fail, so you can
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
from dbt.cli.main import dbtRunner

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS
@task
def reset_raw_tables() -> None:
    with duckdb.connect(DUCKDB_PATH) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # Creates or refreshes the raw.fragments table
        conn.execute("""
            CREATE OR REPLACE TABLE raw.fragments (
                order_no VARCHAR,
                word VARCHAR,
                received_at TIMESTAMP
            )
            """)
        # Creates or refreshes the raw.rejects table
        conn.execute("""
            CREATE OR REPLACE TABLE raw.rejects (
                message_id VARCHAR, 
                attributes VARCHAR,
                received_at TIMESTAMP)""")
@task
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
            logger.info("All delayed messages released")
            return c
        time.sleep(poll_s)
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")


@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    stored = 0

    # Creates a set to store the order numbers we have seen; protects against duplicates
    seen = set()
    deadline = time.monotonic() + timeout_s
    conn = duckdb.connect(DUCKDB_PATH)

    while stored < expected and time.monotonic() < deadline: # Runs until the timeout deadline

        # Recieves messages and polls (checks for new visible messages) every 20 seconds
        resp = sqs.receive_message(
            QueueUrl = queue_url, 
            MaxNumberOfMessages = min(10, expected - stored),
            WaitTimeSeconds = 20,
            MessageAttributeNames = ["All"],
        )

        # Loops through all of the recieved messages
        for message in resp.get("Messages", []):
            receipt = message["ReceiptHandle"]  # Stores receipt handle for deletion
            attributes = message.get("MessageAttributes", {})   # Retrieves message attributes or empty set if none exist
            order_attr = attributes.get("order_no", {})     # Retrieves order number attribute or empty set if none exists
            word_attr = attributes.get("word", {})  # Retrieves word attribute or empty set if none exist

            order_no = order_attr.get("StringValue")    # Collects the order number attribute by reading as a string
            word = word_attr.get("StringValue")     # Collects the word attribute by reading as a string

            # Handles messages with no word
            if word_attr == {}:
                conn.execute("""
                    INSERT into raw.rejects (message_id, attributes, received_at) 
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        """, (order_no, word))
                sqs.delete_message(
                            QueueUrl = queue_url, 
                            ReceiptHandle = receipt)
                logger.info("Message without word deleted")
                continue

            # Handles messages that have already been seen and stored (repeats)
            if order_no in seen:
                conn.execute("""
                    INSERT into raw.rejects (message_id, attributes, received_at) 
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    """, (order_no, word))
                sqs.delete_message(
                    QueueUrl = queue_url, 
                    ReceiptHandle = receipt)
                logger.info("Repeat message received and deleted")
                continue

            # Stores and deletes non-repeat messages that contain the proper attributes
            conn.execute("""
                INSERT into raw.fragments (order_no, word, received_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (order_no, word))
            sqs.delete_message(
                QueueUrl = queue_url, 
                ReceiptHandle = receipt)
            seen.add(order_no) # Adds the order number to the seen set after the message is stroed

            # Collects the number of stored messages and queue counts to log
            stored = conn.execute(
                            "SELECT COUNT(DISTINCT order_no) FROM raw.fragments"
                        ).fetchone()[0]
            counts = get_counts(queue_url)

            # Logs the number of stored messages and queue counts after every read message
            logger.info(
                        "collect_messages - stored %d/%d | visible=%d in_flight=%d delayed=%d",
                        stored,
                        expected,
                        counts["visible"],
                        counts["in_flight"],
                        counts["delayed"],
                    )
            
    # Time out error if the expected number of fragments are not stored by the timeout time
    if stored < expected:
        logger.error(
        "Timed out: stored %d of %d fragments after %d seconds",
        stored, expected, timeout_s
        )
        conn.close()
        raise TimeoutError(f"Failure: {expected} Messages not stored after {timeout_s} s")

    # Logs the final queue counts
    c = get_counts(queue_url)
    logger.info(
        "Final queue counts: visible=%d in_flight=%d delayed=%d",
        c["visible"], c["in_flight"], c["delayed"]
    )

    # If all queue counters are zero, the queue is empty
    if not any(c.values()):
        logger.info(
            "All %d fragments stored; all queue counters are zero",
            stored
        )
    conn.close()  # Closes the connection before returning
    return stored

@task
def dbt_build() -> None:
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")


@task
def read_phrase() -> str:
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
    get_run_logger().info("Assembled %d fragments: %s", n, phrase)
    return phrase


@task(retries=2, retry_delay_seconds=5)
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
    get_run_logger().info("Submitted, HTTP %s", status_code)
    if status_code != 200:
        raise RuntimeError(f"Submission returned HTTP {status_code}")
    return status_code


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    reset_raw_tables()
    collect_messages(queue_url)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)

if __name__ == "__main__":
    dp2_pipeline()
