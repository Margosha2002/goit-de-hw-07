from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import random
import time


def pick_medal():
    return random.choice(["calc_Bronze", "calc_Silver", "calc_Gold"])


def generate_delay():
    time.sleep(35)


def create_table_query():
    return """
        CREATE TABLE IF NOT EXISTS medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        )
    """


def count_medals_query(medal_type):
    return f"""
        INSERT INTO medal_counts (medal_type, count, created_at)
        SELECT '{medal_type}', COUNT(*), NOW()
        FROM olympic_dataset.athlete_event_results
        WHERE medal = '{medal_type}'
    """


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate DAG
dag = DAG(
    "medal_count_dag",
    default_args=default_args,
    description="A DAG to count medals based on type",
    schedule=None,
    start_date=datetime(2023, 12, 1),
    catchup=False,
)

# Task 1: Create table
t1_create_table = SQLExecuteQueryOperator(
    task_id="create_table", conn_id="mysql_default", sql=create_table_query(), dag=dag
)

# Task 2: Random medal selection
t2_pick_medal = PythonOperator(
    task_id="pick_medal", python_callable=pick_medal, dag=dag
)

# Task 3: Branching
t3_pick_medal_task = BranchPythonOperator(
    task_id="pick_medal_task", python_callable=pick_medal, dag=dag
)

# Task 4: Count Bronze medals
t4_calc_bronze = SQLExecuteQueryOperator(
    task_id="calc_Bronze",
    conn_id="mysql_default",
    sql=count_medals_query("Bronze"),
    dag=dag,
)

# Task 5: Count Silver medals
t5_calc_silver = SQLExecuteQueryOperator(
    task_id="calc_Silver",
    conn_id="mysql_default",
    sql=count_medals_query("Silver"),
    dag=dag,
)

# Task 6: Count Gold medals
t6_calc_gold = SQLExecuteQueryOperator(
    task_id="calc_Gold",
    conn_id="mysql_default",
    sql=count_medals_query("Gold"),
    dag=dag,
)

# Task 7: Delay task
t7_generate_delay = PythonOperator(
    task_id="generate_delay", python_callable=generate_delay, dag=dag
)

# Task 8: Check for correctness
t8_check_for_correctness = SqlSensor(
    task_id="check_for_correctness",
    conn_id="mysql_default",
    sql="""
        SELECT COUNT(*)
        FROM medal_counts
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
    """,
    dag=dag,
)

# Task dependencies
t1_create_table >> t2_pick_medal >> t3_pick_medal_task

t3_pick_medal_task >> t4_calc_bronze >> t7_generate_delay >> t8_check_for_correctness

t3_pick_medal_task >> t5_calc_silver >> t7_generate_delay >> t8_check_for_correctness

t3_pick_medal_task >> t6_calc_gold >> t7_generate_delay >> t8_check_for_correctness
