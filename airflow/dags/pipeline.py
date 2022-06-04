from datetime import timedelta
from typing import Any, Dict, List
from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator
from airflow.models import TaskInstance
from airflow.utils.context import Context
from psycopg2.sql import SQL, Identifier


class PostgresGetLoadStateOperator(PostgresOperator):
    ui_color: str = "#dcdcdc"
    def execute(self, context: 'Context'):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        if self.runtime_parameters:
            final_sql = []
            sql_param = {}
            for param in self.runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in self.runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if self.parameters:
                sql_param.update(self.parameters)
            if isinstance(self.sql, str):
                final_sql.append(SQL(self.sql))
            else:
                final_sql.extend(list(map(SQL, self.sql)))
            return self.hook.get_first(final_sql, parameters=sql_param)[0].isoformat()
        else:
            return self.hook.get_first(self.sql, parameters=self.parameters)[0].isoformat()

def alert_failure(context: Context):
    task_instance: TaskInstance = context["task_instance"]
    return task_instance

default_args: Dict[str, Any] = {
    "owner": "faisal",
    "email": "faisalmalikwidyaprasetya@gmail.com",
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(days=1),
    "do_xcom_push": True
}


get_load_state_query: str = """
SELECT MAX(TO_DATE("{}"::TEXT, 'J')) AS load_state
FROM "fact_order_accumulating"
"""

insert_select_query: str = """
INSERT INTO "fact_order_accumulating"
SELECT EXTRACT(JULIAN FROM MAX(o.date)) AS order_date_id,
       EXTRACT(JULIAN FROM MAX(i.date)) AS invoice_date_id,
       EXTRACT(JULIAN FROM MAX(p.date)) AS payment_date_id,
       MAX(o.customer_id) AS customer_id,
       o.order_number,
       i.invoice_number,
       p.payment_number,
       SUM(ol.quantity) AS total_order_quantity,
       SUM(ol.usd_amount) AS total_order_usd_amount,
       EXTRACT(DAY FROM MAX(i.date::TIMESTAMP) - MAX(o.date::TIMESTAMP)) AS order_to_invoice_lag_days,
       EXTRACT(DAY FROM MAX(p.date::TIMESTAMP) - MAX(i.date::TIMESTAMP)) AS invoice_to_payment_lag_days
FROM orders AS o
LEFT JOIN order_lines AS ol
    ON ol.order_number = o.order_number
LEFT JOIN invoices AS i
    ON i.order_number = o.order_number
LEFT JOIN payments AS p
    ON p.invoice_number = i.invoice_number
WHERE o.date >= '{{ ti.xcom_pull(task_ids='get_orders_load_state', key='return_value') }}'::DATE
   OR i.date >= '{{ ti.xcom_pull(task_ids='get_invoices_load_state', key='return_value') }}'::DATE
   OR p.date >= '{{ ti.xcom_pull(task_ids='get_payments_load_state', key='return_value') }}'::DATE
GROUP BY o.order_number,
         i.invoice_number,
         p.payment_number
ON CONFLICT (order_number)
DO UPDATE SET order_date_id = EXCLUDED.order_date_id,
              invoice_date_id = EXCLUDED.invoice_date_id,
              payment_date_id = EXCLUDED.payment_date_id,
              customer_id = EXCLUDED.customer_id,
              invoice_number = EXCLUDED.invoice_number,
              payment_number = EXCLUDED.payment_number,
              total_order_quantity = EXCLUDED.total_order_quantity,
              total_order_usd_amount = EXCLUDED.total_order_usd_amount,
              order_to_invoice_lag_days = EXCLUDED.order_to_invoice_lag_days,
              invoice_to_payment_lag_days = EXCLUDED.invoice_to_payment_lag_days;
"""

with DAG(
    dag_id="data_pipeline",
    description="DAG that process operational database to Data Warehouse",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 3, 7, tz="Asia/Jakarta"),
    on_failure_callback=alert_failure,
    default_args=default_args,
    tags=["pipeline"],
) as dag:
    start: EmptyOperator = EmptyOperator(
        task_id="start"
    )

    get_load_states: List[PostgresGetLoadStateOperator] = [
        PostgresGetLoadStateOperator(
            task_id="get_orders_load_state",
            do_xcom_push=True,
            postgres_conn_id="efishery_task_db",
            sql=get_load_state_query.format("order_date_id")
        ),
        PostgresGetLoadStateOperator(
            task_id="get_invoices_load_state",
            do_xcom_push=True,
            postgres_conn_id="efishery_task_db",
            sql=get_load_state_query.format("invoice_date_id")
        ),
        PostgresGetLoadStateOperator(
            task_id="get_payments_load_state",
            do_xcom_push=True,
            postgres_conn_id="efishery_task_db",
            sql=get_load_state_query.format("payment_date_id")
        ),
    ]
    insert_select: PostgresOperator = PostgresOperator(
        task_id="insert_select",
        postgres_conn_id="efishery_task_db",
        sql=insert_select_query
    )
    end: EmptyOperator = EmptyOperator(
        task_id="end"
    )
    
    start >> get_load_states >> insert_select >> end
