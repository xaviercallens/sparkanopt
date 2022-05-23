#deleting jobs and saved data in databricks
from subprocess import Popen, PIPE
import json
import pandas as pd
import multiprocessing
from tqdm import tqdm
import glob
def job_delete(job_id):
        cmd = ["databricks", "jobs", "delete", "--job-id", job_id]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr=process.communicate()
        print(stderr.decode("utf-8"))

def job_io_delete(path="dbfs:/FileStore/testdfsio/dfsio"):
    cmd = ["dbfs", "rm","-r",path]
    process = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr=process.communicate()
    print(stderr.decode("utf-8"))

    create_io_folder(path)

def create_io_folder(path):
    cmd = ["dbfs", "mkdirs", path]
    process = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr=process.communicate()
    print(stderr.decode("utf-8"))



 




if __name__ == "__main__":
    csv_path = r"C:\Users\sboyalla\Desktop\Internship\data\test_dfsio_large_workers\csv_data\combined.csv"
    df = pd.read_csv(csv_path)
    job_ids = df.job_id.apply(lambda value:str(value)).to_list()
    for i in tqdm(range(len(job_ids))):
        job_delete(job_ids[i])
    job_io_delete()
       

