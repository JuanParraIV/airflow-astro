from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 

## Define our task

def preprocess_data():
    
    print("Preprocessing Data ...")
    
    print("Preprocessing Data  Done")
    
## Define our task 2

def train_model():
    
    print("Training Model ...")
    
    print("Training Model Done")
    
## Define our task 3

def evaluate_model():
    
    print("Evaluating Model ...")
    
    print("Evaluating Model Done")


## Define our DAG

with DAG(
    'ml_pipeline',
    start_date=datetime(2025, 3, 13),
    schedule_interval='@daily'
) as dag:
  
    preprocess_data = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )
    
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    
    evaluate_model = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model
    )
    
    ## set Dependencies
    preprocess_data >> train_model >> evaluate_model