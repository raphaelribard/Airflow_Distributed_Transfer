# Airflow_Distributed_Transfer
This repo shows how to set a distributed transfer using Apache Airflow 

There are 2 DAGs (Directed Acyclic Graph):
    - First DAG : 1 sensor which will look for the file to be transfered to land in the specific location (every X seconds during a certain time window) followed by a task which does the following:
        1. Get the size of the file
        2. Compute the associated number of chunks in relation with the chunk_size (Airflow variable) : number_of_chunks=file_size/chunk_size
        3. Update the number_of_chunks variable with the correct number
        4. Clean the destination folder
        5. Trigger the second DAG
    - Second DAG : 
        1. Parallel tasks are dynamically created using the Airflow variable "number_of_chunks" : each of these parallel tasks is in charge of copying a specific chunk of the file in the associated location of the output file.
        2. The last task is to clean the source folder
        
