#! /usr/bin/env python
# -*- coding: utf-8 -*-

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
    'start_date': datetime(2024, 5, 14),
    'email': ['evgdmitriev@ozon.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=180)
}

dag = DAG('lssup_wh_team_active', default_args=DEFAULT_ARGS, schedule_interval="00 7 * * *",
          dagrun_timeout=timedelta(minutes=120))

sql_00 = u"""
TRUNCATE TABLE lssup_team.lssup_wh_team_active
"""

sql_01 = u"""
INSERT INTO lssup_team.lssup_wh_team_active (
select 'https://jit.o3.ru/browse/' || project.pkey || '-' || jit.issuenum as 'Task'
, status.pname as 'State'
, jit.summary as 'Summary'
, case when datediff('day', jit.updated, now()) > 14 then 'Update it' else 'Updated' end as 'LastUpdate'
, case when datediff('day',now(), jit.duedate) < 0 then 'Overdue' else 'Actual' end as 'DueDate'
, au.lower_user_name as 'Usr'
from jitdb.jiraissue jit
join jitdb.project project on project.id = jit.project
join jitdb.label label on label.issue = jit.id
join jitdb.issuestatus status on status.id = jit.issuestatus
join jitdb.app_user au on au.user_key = jit.assignee 
where 1=1
and label.label in ('aovsienko', 'bkaraulov', 'rvdovenko', 'dtrishin')
and status.pname != 'Closed'
order by au.lower_user_name desc
)
"""

task_0 = VerticaOperator(
    task_id='lssup_wh_team_active_truncate',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_00,
    dag=dag
)

task_1 = VerticaOperator(
    task_id='lssup_wh_team_active_insert',
    vertica_conn_id='lssup_connection_to_vertica',
    sql=sql_01,
    dag=dag
)

yamlpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/lssup_wh_team_active.yaml')
# export, wait = export_to_marts('lssup_cc_flow.export', dag, yamlpath, 'full')
export, wait = export_to_marts(
    task_id='lssup_wh_team_active.export',
    dag=dag,
    config_path=yamlpath,
    mode='full',
    client_id=get_client_id(),
    client_secret=get_s2s_client_secret()
)

task_0 >> task_1 >> export