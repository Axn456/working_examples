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
    'start_date': datetime(2024, 7, 16),
    'email': ['evgdmitriev@ozon.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=180)
}

dag = DAG('lssup_wh_team_pm', default_args=DEFAULT_ARGS, schedule_interval="00 6 * * *",
          dagrun_timeout=timedelta(minutes=120))

sql_00 = u"""
TRUNCATE TABLE lssup_team.lssup_wh_team_pm
"""

sql_01 = u"""
INSERT INTO lssup_team.lssup_wh_team_pm (
select
  'https://jit.o3.ru/browse/' || pr.pkey || '-' || jj.issuenum as Task
, jj.summary  					as Summary
, jj.created::date				as Createddate
, jj.resolutiondate::date		as Resolveddate
, jj.duedate::date				as Duedate
, jitstate.pname              	as State                  
, au.lower_user_name			as Login
from jitdb.jiraissue j
join jitdb.issuelink il on il.source = j.id
join jitdb.jiraissue jj on jj.id = il.destination
join jitdb.issuestatus jitstate on jitstate.id = jj.issuestatus
join jitdb.project pr on pr.id = jj.project
join jitdb.app_user au on au.user_key = jj.assignee 
where j.issuenum = 874 and j.project = 20129
union 
select
  'https://jit.o3.ru/browse/' || p.pkey || '-' || j.issuenum 		as Task
, j.summary 						as Summary
, j.created::date					as Createddate
, j.resolutiondate::date			as Resolveddate
, j.duedate::date                	as Duedate
, jitstate.pname                    as State 
, au.lower_user_name				as Login
from jitdb.label l
join jitdb.jiraissue j on j.id = l.issue
join jitdb.project p on p.id = j.project
join jitdb.issuestatus jitstate on jitstate.id = j.issuestatus
join jitdb.customfieldvalue crt on crt.issue = j.id and crt.customfield =19600
join jitdb.app_user au on au.user_key = crt.stringvalue 
where l.label in ('LS_WH_PM') 
)
"""

task_0 = VerticaOperator(
    task_id='lssup_wh_team_pm_truncate',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_00,
    dag=dag
)

task_1 = VerticaOperator(
    task_id='lssup_wh_team_pm_insert',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_01,
    dag=dag
)

yamlpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/lssup_wh_team_pm.yaml')
export, wait = export_to_marts(
    task_id='lssup_wh_team_pm.export',
    dag=dag,
    config_path=yamlpath,
    mode='full',
    client_id=get_client_id(),
    client_secret=get_s2s_client_secret()
)

task_0 >> task_1 >> export