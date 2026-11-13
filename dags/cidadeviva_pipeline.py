"""DAG principal do pipeline de dados do CidadeViva.

           ┌─ ingerir_sp156 ────► silver_sp156 ─────┐
 inicio ───┼─ ingerir_cidadeviva ► silver_cidadeviva ┼─► gold ─► qualidade ─► carregar_dw ─► fim
           └─ ingerir_referencias ──────────────────┘

* agendada diariamente (@daily); `catchup=False` para a demo, mas toda task é idempotente por data lógica (ds);
* as tasks só passam a data lógica — os dados trafegam pelo data lake (bronze/silver/gold), não por XCom;
* `qualidade` bloqueia a carga no DW se alguma verificação falhar.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from pipeline import gold, ingest, load, quality, transform

default_args = {
    "owner": "citizentech-cidadeviva",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _t(fn):
    """Adapta a função do pacote para receber a data lógica do Airflow (`ds`)."""
    def _run(ds: str, **_):
        return fn(ds)
    _run.__name__ = fn.__name__
    return _run


with DAG(
    dag_id="cidadeviva_pipeline",
    description="SP156 + reportes do app CidadeViva -> bronze/silver/gold -> Data Warehouse",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["citizentech", "cidadeviva", "etl"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim")

    ingerir_sp156 = PythonOperator(task_id="ingerir_sp156", python_callable=_t(ingest.ingerir_sp156))
    ingerir_cidadeviva = PythonOperator(task_id="ingerir_cidadeviva", python_callable=_t(ingest.ingerir_cidadeviva))
    ingerir_referencias = PythonOperator(task_id="ingerir_referencias", python_callable=_t(ingest.ingerir_referencias))

    silver_sp156 = PythonOperator(task_id="silver_sp156", python_callable=_t(transform.silver_sp156))
    silver_cidadeviva = PythonOperator(task_id="silver_cidadeviva", python_callable=_t(transform.silver_cidadeviva))

    construir_gold = PythonOperator(task_id="construir_gold", python_callable=_t(gold.construir_gold))
    validar_qualidade = PythonOperator(task_id="validar_qualidade", python_callable=_t(quality.validar_gold))
    carregar_dw = PythonOperator(task_id="carregar_dw", python_callable=_t(load.carregar_warehouse))

    inicio >> [ingerir_sp156, ingerir_cidadeviva, ingerir_referencias]
    # silver precisa também das referências (subprefeituras/regras)
    [ingerir_sp156, ingerir_referencias] >> silver_sp156
    [ingerir_cidadeviva, ingerir_referencias] >> silver_cidadeviva
    [silver_sp156, silver_cidadeviva] >> construir_gold >> validar_qualidade >> carregar_dw >> fim
