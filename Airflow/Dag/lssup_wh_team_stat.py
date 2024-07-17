
from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
from datetime import datetime, timedelta
from dreadnought_package.operators import export_to_marts
import os
from airflow.operators.dummy_operator import DummyOperator
from dreadnought_package.operators import export_to_marts
from de_airflow_utils.s2s.function import get_s2s_client_secret, get_client_id

DEFAULT_ARGS = {
    'owner': 'lssup_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 6),
    'email': ['evgdmitriev@ozon.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=180)
}

dag = DAG('lssup_wh_team_stat', default_args=DEFAULT_ARGS, schedule_interval="00 6 * * *",
          dagrun_timeout=timedelta(minutes=120))

sql_00 = u"""
TRUNCATE TABLE lssup_team.lssup_wh_team_stat
"""

sql_01 = u"""
INSERT INTO lssup_team.lssup_wh_team_stat (
select r.name as 'RequestType'
, j.created::date as 'JiraCreatedDate'
, j.resolutiondate::date as 'JiraResolveDate'
, au.lower_user_name as 'Login'
from prodjiradb.jiraissue j
join prodjiradb."AO_54307E_VIEWPORT" po on po."PROJECT_ID" = j.project and j.project = 16601
join prodjiradb."AO_54307E_VIEWPORTFORM" r on po."ID" = r."VIEWPORT_ID"
join prodjiradb.customfieldvalue crt on crt.issue = j.id and crt.customfield = 12700 
join prodjiradb.customfieldvalue team on team.issue = j.id and team.customfield = 17032 
join prodjiradb.app_user au on au.user_key = j.assignee 
where  (po."KEY" || '/' || r."KEY") = crt.stringvalue
and date_trunc('year', j.created) = date_trunc('year', current_date) 
and team.stringvalue IN ('lssup_wh_team', 'ls_wh_support_team')
)
"""

task_0 = VerticaOperator(
    task_id='lssup_wh_team_stat_truncate',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_00,
    dag=dag
)

task_1 = VerticaOperator(
    task_id='lssup_wh_team_stat_insert',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_01,
    dag=dag
)

yamlpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/lssup_wh_team_stat.yaml')
export, wait = export_to_marts(
    task_id='lssup_wh_team_stat.export',
    dag=dag,
    config_path=yamlpath,
    mode='full',
    client_id=get_client_id(),
    client_secret=get_s2s_client_secret()
)

task_0 >> task_1 >> export