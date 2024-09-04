##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################
encode_features = PythonOperator(task_id='encoding_categorical_variables', 
                            python_callable=utils.encode_features,
                            op_kwargs={'db_path': db_path, 'db_file_name': db_file_name, 
                                       'one_hot_encoded_features': one_hot_encoded_features, 'features_to_encode': features_to_encode},
                            dag=ML_training_dag)
###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################
get_trained_model = PythonOperator(task_id='training_model', 
                            python_callable=utils.get_trained_model,
                            op_kwargs={'db_path': db_path, 'db_file_name': db_file_name,
                                       'model_config': model_config, 'experiment': experiment, 'tracking_uri': tracking_uri},
                            dag=ML_training_dag)

###############################################################################
# Define relations between tasks
# ##############################################################################

encode_features.set_downstream(get_trained_model)