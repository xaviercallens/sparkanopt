from multiprocessing import Pipe

from xml.etree.ElementTree import PI
import pandas as pd
import json
from tqdm import tqdm
from subprocess import PIPE, Popen
import  sys

job_data = pd.read_csv(r"C:\Users\sboyalla\Desktop\Internship\data\final_data.csv")
print(job_data[job_data['job_name'] == 'test_dfsio_Standard_D8ds_v4_2_60'])




def db_authenticator(input_file, db_workspace="api/2.1/jobs/list"):
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

def get_jobs_details(path):
    cmd = ['dbfs', "ls", path]
    process = Popen(cmd, stdout=PIPE, stderr=PIPE)
    output = process.communicate()[0].decode("utf-8")
    
    return output


host , header = db_authenticator(input_file=r'C:\Users\sboyalla\.databrickscfg')

job_data = pd.read_csv(r"C:\Users\sboyalla\Desktop\Internship\data\test_dfsio.csv") #csv with job_name or job_id
job_name = job_data['job_name'].tolist()
cl_ids = {"cluster_id":[]}
for i in tqdm(job_name):
    path = "dbfs:/FileStore/cluster-logs-2/{}".format(i)
   
    id = get_jobs_details(path)
    cl_ids['cluster_id'].append(id.rstrip())
cl_df = pd.DataFrame.from_dict(cl_ids)
job_data["cluster_id"] =cl_df["cluster_id"]
job_data.to_csv("job_data_wcl.csv")

