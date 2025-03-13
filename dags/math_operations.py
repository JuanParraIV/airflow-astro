"""
  We'll define a DAG where the tasks are as follows:
  
  Task 1: Start with an initial number (e.g. 10).
  Task 2: Add 5 to the number.
  Task 3: Multiply the result by 2.
  Task 4: Subtract 3 from the result.
  Task 5: Compute the square of the result.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define functions for each task

def start_number(**context):
    """
    Task 1: Start with an initial number (e.g. 10).
    Push the initial number to XCom.
    """
    context['ti'].xcom_push(key='current_val', value=10) # Pushing the value to XCom
    print("Starting Number 10..")
    
def add_five(**context):
    """
    Task 2: Add 5 to the number.
    Pull the current value from XCom, add 5, and push the new value to XCom.
    """
    current_val = context['ti'].xcom_pull(key='current_val', task_ids='start_number_task') # Pulling the value from XCom
    new_value =  current_val + 5 # Adding 5 to the current value
    context['ti'].xcom_push(key='current_val', value=new_value) # Pushing the new value to XCom
    print(f"Adding 5 to the number {current_val} gives {new_value}") # Printing the result
    
def multiply_by_two(**context):
    """
    Task 3: Multiply the result by 2.
    Pull the current value from XCom, multiply by 2, and push the new value to XCom.
    """
    current_val = context['ti'].xcom_pull(key='current_val', task_ids='add_five_task') # Pulling the value from XCom
    new_value = current_val * 2 # Multiplying the current value by 2
    context['ti'].xcom_push(key='current_val', value=new_value) # Pushing the new value to XCom
    print(f"Multiplying the number {current_val} by 2 gives {new_value}") # Printing the result
    
def subtract_three(**context):
    """
    Task 4: Subtract 3 from the result.
    Pull the current value from XCom, subtract 3, and push the new value to XCom.
    """
    current_val = context['ti'].xcom_pull(key='current_val', task_ids='multiply_by_two_task') # Pulling the value from XCom
    new_value = current_val - 3 # Subtracting 3 from the current value
    context['ti'].xcom_push(key='current_val', value=new_value) # Pushing the new value to XCom
    print(f"Subtracting 3 from the number {current_val} gives {new_value}") # Printing the result
    
def square_number(**context):
    """
    Task 5: Compute the square of the result.
    Pull the current value from XCom, compute the square, and push the new value to XCom.
    """
    current_val = context['ti'].xcom_pull(key='current_val', task_ids='subtract_three_task') # Pulling the value from XCom
    new_value = current_val ** 2 # Computing the square of the current value
    context['ti'].xcom_push(key='current_val', value=new_value) # Pushing the new value to XCom
    print(f"The square of the number {current_val} is {new_value}") # Printing the result
    

## Define our DAG

with DAG(
  dag_id='math_sequence_dag',
  start_date=datetime(2025, 3, 13),
  schedule_interval='@daily',
  catchup=False
) as dag:
  
  start_number_task = PythonOperator(
    task_id='start_number_task',
    python_callable=start_number,
    provide_context=True
  )
  
  add_five_task = PythonOperator(
    task_id='add_five_task',
    python_callable=add_five,
    provide_context=True
  )
  
  multiply_by_two_task = PythonOperator(
    task_id='multiply_by_two_task',
    python_callable=multiply_by_two,
    provide_context=True
  )
  
  subtract_three_task = PythonOperator(
    task_id='subtract_three_task',
    python_callable=subtract_three,
    provide_context=True
  )
  
  square_number_task = PythonOperator(
    task_id='square_number_task',
    python_callable=square_number,
    provide_context=True
  )
  
  ## Set Dependencies
  start_number_task >> add_five_task >> multiply_by_two_task >> subtract_three_task >> square_number_task