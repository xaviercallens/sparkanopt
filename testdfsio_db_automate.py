### This script automatically launches the test_dfsio jobs and collects the data ###
### written by SH BOYALLA ###

from cProfile import run
from importlib.resources import path
from json.tool import main
from multiprocessing import pool
import requests
import json
from subprocess import Popen, PIPE
import subprocess
import logging
import itertools
import os
import time
import pandas as pd
import sys
from tqdm import tqdm
import argparse


##adding the comments part
parser = argparse.ArgumentParser(description='Getting instance ids and pools')
parser.add_argument('--pool_instance_id', required=True,type=str, help="Id of the pool instance on which the jobs have to be run")
parser.add_argument("--cluster_id",  required=True,type=str, help="Id of the cluster")
parser.add_argument("--saving_path", required=True,type=str, help="csv path to save the path")
args = parser.parse_args()
pool_instance_id = args.pool_instance_id
cluster_id = args.cluster_id
csv_path = args.saving_path


def db_authenticator(input_file, db_workspace="api/2.0/jobs/create"):
    """    The funtion returns the host and token in necesasary format to authenticate databricks.
   
    Input:

    Takes input the path to dbfsconfig file, db_workspace type and the json file
   
    output: 
    
    returns http response of databricks website.

    """
    dict = {}
    with open(input_file, "r") as f:
        for line in f:
            l = line.strip().split(" = ", 1)
            if l[0] == 'host' or l[0] == 'token':
                dict[l[0]] = l[1]
    dict['host'] = dict["host"].strip('?o=2076517530543986#')+db_workspace
    headers = {'Authorization': 'Bearer %s' % dict['token'] }
    return dict["host"],headers

def  create_job_json(job_config, job_name, dbfs_path):
    
    """
    This fuction creates the json configuration file necesarry to launch a job.

    Input:
          1). job_config - parameters to create a json file .
    
    returns:
           A json file containing job specs.
    """
    cl_type, fls, n_w, pl_id = job_config
    name = job_name
    fls = str(fls*1000000)



    job_json =  {"name": name,
    "new_cluster": {
    "spark_version":"9.1.x-scala2.12",
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "instance_pool_id": pl_id,
     "num_workers": n_w,
      "cluster_log_conf":{
          "dbfs":{
               "destination": "dbfs:/FileStore/test_dfsio_cluster_logs_large_files_high_workers/"+name
          }
      }

     
       },
       "spark_submit_task": {
       "parameters": ["--class",
       "com.msft.dfsio.DFSIOSpark",
       "dbfs:/FileStore/testdfsio/jar_files/spark_dfsio_1_0_SNAPSHOT.jar",
       n_w,fls,
       dbfs_path,
       "4096",
        "both"]}}
    return job_json

def db_post_requestor(host, header, job_json):
    """
    1). jobs_json - json file with info about job.
    """
    resp = requests.post(host, headers=header, json=job_json)
    return resp

def run_job(job_id):
   
    cmd = ["databricks", "jobs", "run-now",  "--job-id", str(job_id)]
    run_process = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = run_process.communicate()
    run_id = json.loads(stdout.decode("utf-8"))['run_id']
    cluster_id = get_status(run_id=run_id)
    return run_id, cluster_id

def get_status(run_id):

    """
        checks   the runnning status of a job , and waits till the completion.
        Input: 
        
            1). run_id

        returns: 
           cluster_id on which the job is run.         

           
    """
    cmd = ['databricks', 'runs','get','--run-id', str(run_id)]
    get_id =  Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = get_id.communicate()
    stdout_json =  json.loads(stdout.decode("utf-8"))
    end_time = stdout_json['tasks'][0]['end_time']
    if end_time > 0:
        print('Exiting the wait time now')
        cluster_id = stdout_json['tasks'][0]['cluster_instance']['cluster_id']
        return cluster_id
    else:
        print('Entering wait time!!!!!!')
        time.sleep(10)
        try:
            return get_status(run_id=run_id)
        except BaseException:
            return 0
        

        

  

    

def job_spec(cluster_type, n_workers, filesize, instance_pools):
    """
    Input:
       cluster_type: list of cluster types

       n_workers_range:range of workers for cluster type.

       filesize: list of file sizes

       instance_pool_id : id of the instance pool 

    output:
         returns a list containing job specification(tuple) to be used in job json
    """

    base_c = [[cluster_type[index], fls,nw, instance_pools[index]] for index in range(len(cluster_type))   for fls  in filesize  for nw in n_workers]
   
   
    return base_c

def create_log_folder(folder_path):
    """
       Input:

       1). Takes the azure databricks path where the folder has to be created.

       creates a folder in azure databricks dbfs to dump log files.
    
    """

    cmd = 'dbfs mkdirs '+folder_path
    try:
        #demo
        subprocess.call(cmd)
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured with create_job:", exc_info = True)

def save_data(path, data):
    """
           save the job_json data
    """
    
    if not os.path.exists(path):
            job_data = pd.DataFrame.from_dict(data)
            job_data.to_csv(path, index=False)
    else:
        job_csv = pd.read_csv(path)
        job_data = pd.DataFrame.from_dict(data)
        final = pd.concat([job_csv, job_data])
        final.to_csv(path, index=False)

def create_db_files_folder(job_name, dbfs_path="dbfs:/FileStore/testdfsio/dfsio"):
    #deleting the files created by the job in databricks after finishing the run
    dbfs_path = dbfs_path+"/"+job_name
    
    cmd_create_folder = ["dbfs", "mkdirs", dbfs_path]
    print("creating the file folder!!!")
    creation_process = Popen(cmd_create_folder, stderr=PIPE, stdout=PIPE)
    stdout, stderr = creation_process.communicate()
    return dbfs_path

 

    







if __name__ == "__main__":

 # Below code creates the job_specification
   
    
 
    n_workers = [1,5,10,25]
    cluster_type= [cluster_id]
    filesize = [512, 1024]
    instance_pools = [pool_instance_id]
    
    final_spec = job_spec(cluster_type=cluster_type, n_workers=n_workers,filesize=filesize, instance_pools=instance_pools)
    host , header = db_authenticator(input_file=r'C:\Users\sboyalla\.databrickscfg')
    job_data = {'job_name':[],'cluster_type':[], 'file_size':[], 'n_workers':[], 'job_id':[],  'run_id':[], 'cluster_id':[]}
    no_of_jobs_finished = 0
    print(final_spec)
#creating the jobs
    
    for i in tqdm((range(len(final_spec)))):

   
            print()
            name = 'test_dfsio_2_'+final_spec[i][0]+"_nworkers_"+str(final_spec[i][2])+"_filesize_"+str(final_spec[i][1])
            print(str(i), ").Running the job -- ",name)
            job_data["cluster_type"].append(final_spec[i][0])
            job_data["file_size"].append(final_spec[i][1])
            job_data['n_workers'].append(final_spec[i][2])
            job_data['job_name'].append(name)
            
            
    #creating the necessary folders
            """"
            log_path = "dbfs:/FileStore/test-dfsio-cluster-logs-5/"+name
            print("creating  the log file in dbfs now")
            

            create_log_folder(folder_path= log_path)
            """
            dbfs_path = create_db_files_folder(job_name=name)

            print("created the necessary folders to write and read")

    #calling  the methods that create the job json and db_requestor to create the job
            print("Creating the job json and job in databricks!!!!!")
            
            job_json = create_job_json(final_spec[i], job_name=name, dbfs_path=dbfs_path) #creating a json of job spec
            
            response = db_post_requestor(host=host, header=header, job_json=job_json) #http request to db website
            
            job_id = response.json()['job_id'] #created the job
            

               
       

        #running the job
            print("Running the job!!!!!")

            run_id, cluster_id = run_job(job_id=job_id)
            no_of_jobs_finished+=1


        #finished running the job , saving the data in a job_data dict so that can be saved to a csv in the end
            job_data['job_id'].append(job_id)
            job_data["run_id"].append(run_id)
            job_data["cluster_id"].append(cluster_id)
            if no_of_jobs_finished == 5:
                print('saving data!!!!!!!!')
                print(job_data)
                

                save_data(csv_path, job_data)
                no_of_jobs_finished = 0
                for key in job_data:
                    job_data[key] = []

           
            
            print()
            print()
    print('saving data after exiting the loop !!!!!!!!')
    save_data(csv_path, job_data)
    
    

