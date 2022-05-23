###################This script reads log data#################
"""
This script reads log data of a job, extracts 
information and saves into a final csv.

"""

from concurrent.futures import process
import imp
from logging import exception
from operator import contains
import pandas as pd 
import subprocess
import requests
import time
from tqdm import tqdm
import sys

def save_logs(*args):
    """
           This method saves the stdout logs of jobs that ran in databricks.
    """
    cmd = "dbfs cp {} {}".format(dbfs_path, dir_path)
    print(cmd)
    subprocess.call(cmd)

def read_data_and_save(paths = []):
    main_path, read_csv_path, dbfs_path, csv_saving_path = paths
    print(main_path, logs_dbfs_path, read_csv_path, saving_csv_path)
    quit()
    job_data = pd.read_csv(main_csv_path)
    job_names = job_data['job_name'].tolist()
    cluster_id = job_data['cluster_id'].tolist()
    metric_data = {'write_time':[],'write_throughput':[], "read_time":[],"read_throughput":[]
    , "overall_write_time":[], "overall_read_time":[]  }
    for i in tqdm(range(len(job_names))):
        job_name = job_names[i]
        cl_id = cluster_id[i]
        dbfs_path = "{}/{}/{}/driver/stdout".format(dbfs_path,job_name, cl_id) #path to cluster logs in datbricks
        
        dir_path = r"C:\Users\sboyalla\Desktop\Internship\data\test_dfsio_large_workers\cluster_logs\{}.txt".format(job_name) #create this path
        
        with open(dir_path, "r") as f:
            tws = 'Benchmark: Total write time (Sum time across all files) :'
            tp_w = 'Benchmark: Overall observed write throughput : '
            trs = 'Benchmark: Total read time (Sum time across all files) :'
            tp_r = 'Benchmark: Overall observed read throughput :'
            owt = "Benchmark: Overall observed write wall time :"
            ort = "Benchmark: Overall observed read wall time :"

            
            for line in f.readlines():
                if tws in line:
                    t = float(line.split(":")[-1].split(" ")[1])
                    metric_data["write_time"].append(t)
                elif tp_w in line:
                    t = float(line.split(":")[-1].split(" ")[1])
                    metric_data["write_throughput"].append(t)
                elif  trs in line:
                    t = float(line.split(":")[-1].split(" ")[1])
                    metric_data["read_time"].append(t)
                elif tp_r in line:
                    t = float(line.split(":")[-1].split(" ")[1])
                    metric_data["read_throughput"].append(t)
                elif owt in line:
                     t = float(line.split(":")[-1].split(" ")[1])
                     metric_data["overall_write_time"].append(t)
                elif ort in line:
                     t = float(line.split(":")[-1].split(" ")[1])
                     metric_data["overall_read_time"].append(t)
    metric_data = pd.DataFrame.from_dict(metric_data)
    final_data = job_data.join(metric_data)
    final_data.to_csv(r'C:\Users\sboyalla\Desktop\Internship\data\test_dfsio_large_workers\test_dfsio_large_workers.csv') #saving csv path
    


    return None

if __name__ == "__main__" :
    process_name = "test_dfsio_cluster_logs_large_files_high_workers"
   
    main_path = r"C:\Users\sboyalla\Desktop\Internship\data\{}".format(process_name)
    
    logs_dbfs_path = r"dbfs:/FileStore/{}".format(process_name) #change the path here
   
    read_csv_path = r"{}\csv_data\combined.csv".format(main_path)
    
    saving_csv_path = read_csv_path.strip("csv_data\combined.csv")+"{}.csv".format(process_name)
    
    print(main_path, logs_dbfs_path, read_csv_path, saving_csv_path)
    
    read_data_and_save([main_path,read_csv_path, logs_dbfs_path, saving_csv_path]) #combined csv path





