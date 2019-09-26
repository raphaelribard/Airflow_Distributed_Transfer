# Airflow_Distributed_Transfer
This repo shows how to set a distributed transfer using Apache Airflow 

There are 2 DAGs (Directed Acyclic Graph):
* First DAG : 
    * Taks 1: 1 sensor which will look for the file to be transfered to land in the specific location (every X seconds during a certain time window)
    * Task 2 : Clean the destination folder
    * Task 3: 
        1. Get the size of the file
        2. Compute the associated number of chunks in relation with the chunk_size (Airflow variable) : number_of_chunks=file_size/chunk_size
        3. Update the number_of_chunks variable with the correct number
     * Task 4: Trigger the second DAG
* Second DAG : 
    * Task 1 : Create empty output file where all the workers will be writing to simulataneously
    * Parallel Tasks 2 : Parallel tasks are dynamically created using the Airflow variable "number_of_chunks" : each of these parallel tasks is in charge of copying a specific chunk of the file in the associated location of the output file.
    * Task 3:  Clean the source folder
    
    
        
