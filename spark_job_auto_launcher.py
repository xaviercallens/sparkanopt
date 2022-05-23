#This script controls the testdfsio_db_automate to run parallely depeninding
#on the number of cores available using the mutliprocessing library


import csv
from importlib.resources import path
from subprocess import Popen, PIPE
from sys import stderr
import json
import multiprocessing
import time
import glob
import pandas as pd
import os
import shutil



def create_necessary_folder( main_folder_name,main_path = r"C:\Users\sboyalla\Desktop\Internship\data"):
    main_f_n = main_path +"\{}".format(main_folder_name)
    log_path = main_f_n+"\process_logs"
    csv_path = main_f_n+"\csv_data"
    path_lists = [main_f_n, log_path, csv_path]
    if os.path.exists(main_f_n):
        shutil.rmtree(main_f_n)

    for path in path_lists:
        os.mkdir(path)
    return path_lists


def job_launcher(cl_id, pool_id, csv_path, log_path):
            
            cmd = ["python", "testdfsio_db_automate.py", "--cluster_id",cl_id,"--pool_instance_id",pool_id, "--saving_path", csv_path]

            process = Popen(cmd, stderr=PIPE, stdout=PIPE)
            stdout, stderr = process.communicate()
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
            with open(log_path, "w") as f:
                f.write(stdout.rstrip())

def check_process_status(process, csv_path):
    i = 0
    print()
    print()
    print("Checking the status of the process!!!!!!!!!")
    print()
    for proc in process:
        if proc.is_alive():
            print("{}, with id {} is alive".format(proc.name, proc.pid))
        else:
            i+=1
    if i == len(process):
        print("Finished all the process")
        print("Combining all the csv files to one")
        combining_all_the_csv(csv_path)
        pass
    else:
        time.sleep(60)
        check_process_status(process, csv_path)

def combining_all_the_csv(main_folder):
    paths = glob.glob(main_folder+"\*.csv")
    df = pd.concat(map(pd.read_csv, paths), ignore_index=False)
    df.to_csv(main_folder+"/combined.csv")
    print("Finsished combining the all the csv")








if __name__ == "__main__":

    process = []
    with open(r"C:\Users\sboyalla\Desktop\Internship\data\created_instance_pools.json", "r") as f:
        pool_instance = json.load(f)
        
        
    main_path, log_path, csv_path = create_necessary_folder(main_folder_name="test_dfsio_large_workers")
    
    for key, value in pool_instance.items():
            csv_path_current = csv_path+"\{}.csv".format(key)
            log_path_current = log_path+"\{}.txt".format(key)
            print(csv_path_current)
            print(log_path_current)
    
    
            p = multiprocessing.Process(target=job_launcher, args=[key, value, csv_path_current, log_path_current])
            p.start()
            process.append(p)
            
            
          
    for proc in process:
        print(proc)
    print()
    print()
    check_process_status(process, csv_path)
 
    for proc in process:
        proc.join()
    