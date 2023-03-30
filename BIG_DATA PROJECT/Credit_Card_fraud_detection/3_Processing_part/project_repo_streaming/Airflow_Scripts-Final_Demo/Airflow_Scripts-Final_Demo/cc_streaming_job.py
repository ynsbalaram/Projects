from airflow import DAG
from airflow.utils.dates import  days_ago
from airflow.utils.dates import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import settings
from airflow.models import Connection

dag = DAG(
dag_id='CC_Streaming_Job_Trigger',
#schedule_interval='0 */8 * * *',
schedule_interval=None,
start_date=days_ago(1)
)

trigger_task = BashOperator(
task_id='cc_streaming_job_trigger_task',
bash_command = 'echo Stareaming Job is Starting',
dag = dag
)

def cc_streaming_job_cmd():
  command_one = f'cd Desktop/Softwares/spark-2.4.3-bin-hadoop2.7/bin'
  command_two = './spark-submit --class classReadFromKafka /home/cloudera/Desktop/Demo_Final.jar'
  return f'{command_one} && {command_two}'

cc_streaming_job = SSHOperator(
 task_id='cc_streaming_job',
 ssh_conn_id='cloudera',
 command=cc_streaming_job_cmd(),
 dag=dag
) 

dummy = DummyOperator(
 task_id='dummy',
 dag = dag 
 )

trigger_task >> cc_streaming_job >> dummy
