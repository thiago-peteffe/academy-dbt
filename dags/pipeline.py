from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

dbt_project_dir = Variable.get("dbt_project_dir")
dbt_profiles_dir = Variable.get("dbt_profiles_dir")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='orquestracao_dbt',
    default_args=default_args,
    description='Orquestração de comandos dbt usando Airflow',
    schedule_interval='0 0 * * *',  # Diariamente às 00:00
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'''
        cd {dbt_project_dir} &&
        dbt run --models staging --profiles-dir {dbt_profiles_dir}
        '''
    )

    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command=f'''
        cd {dbt_project_dir} &&
        dbt test --models staging --profiles-dir {dbt_profiles_dir}
        '''
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'''
        cd {dbt_project_dir} &&
        dbt run --models marts --profiles-dir {dbt_profiles_dir}
        '''
    )

    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command=f'''
        cd {dbt_project_dir} &&
        dbt test --models marts --profiles-dir {dbt_profiles_dir}
        '''
    )

    # Define a ordem das tarefas
    dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts
