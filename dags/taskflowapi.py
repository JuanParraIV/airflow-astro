""" 
Apache Airflow introduced the TaskFlow API which allows you to create tasks using Python Decorators like @tasks. 
This is a cleaner and more intuitive way of writing tasks without needing to manually use operators like PythonOperator.
Let me show you how to modify the previous code to use the @task decorator.
"""

from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime


## Define DAG
with DAG(
  dag_id='math_sequence_dag_with_taskflow',
  start_date=datetime(2025, 3, 13),
  schedule_interval='@once',
  catchup=False
) as dag:

  @task
  def start_number():
    """
    Task 1: Start with the number 5.
    This task initializes the pipeline by setting the current value to 5.
    """
    current_val = 5 # Initializing the current value
    print(f"Starting with the number {current_val}") # Printing the current value
    return current_val # Returning the current value

  @task
  def add_five(current_val: int) -> int:
    """
    Task 2: Add 5 to the result.
    This task pulls the current value from XCom, adds 5, and pushes the new value to XCom.
    """
    new_value = current_val + 5 # Adding 5 to the current value
    print(f"Adding 5 to the number {current_val} gives {new_value}") # Printing the result
    return new_value # Returning the new value

  @task
  def multiply_by_two(current_val: int) -> int:
    """
    Task 3: Multiply the result by 2.
    This task pulls the current value from XCom, multiplies by 2, and pushes the new value to XCom.
    """
    new_value = current_val * 2 # Multiplying the current value by 2
    print(f"Multiplying the number {current_val} by 2 gives {new_value}") # Printing the result
    return new_value # Returning the new value

  @task
  def subtract_three(current_val: int) -> int:
    """
    Task 4: Subtract 3 from the result.
    This task pulls the current value from XCom, subtracts 3, and pushes the new value to XCom.
    """
    new_value = current_val - 3 # Subtracting 3 from the current value
    print(f"Subtracting 3 from the number {current_val} gives {new_value}") # Printing the result
    return new_value # Returning the new value

  @task
  def square_number(current_val: int) -> int:
    """
    Task 5: Compute the square of the result.
    This task pulls the current value from XCom, computes the square, and pushes the new value to XCom.
    """
    new_value = current_val ** 2 # Computing the square of the current value
    print(f"The square of the number {current_val} is {new_value}") # Printing the result
    return new_value # Returning the new value
  
  
  # Define the task dependencies
  start_number_task = start_number()
  add_five_task = add_five(start_number_task)
  multiply_by_two_task = multiply_by_two(add_five_task)
  subtract_three_task = subtract_three(multiply_by_two_task)
  square_number_task = square_number(subtract_three_task)
  