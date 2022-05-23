from asyncio.subprocess import STDOUT
from multiprocessing import Pipe
from subprocess import PIPE,Popen
import json
from xml.etree.ElementTree import PI
import requests



cmd = "databricks instance-pools list --output json"
cmd = cmd.split(" ")

process = Popen(cmd, stderr=PIPE, stdout=PIPE, shell=True)
stdout = process.communicate()[0].decode('utf-8')
instance_dict = json.loads(stdout)
instance_pools = {}
for i in range(1,11):
    main_path =instance_dict['instance_pools'][i]
    instance_pool_cl_type = main_path['node_type_id']
    instance_pool_name = main_path['instance_pool_name']
    instance_pool_cl_id = main_path['instance_pool_id']
    instance_pools[instance_pool_cl_type] = instance_pool_cl_id
    print(i, instance_pool_name, instance_pool_cl_type, instance_pool_cl_id)
with open('created_instance_pools.json', "w") as f:
    json.dump(instance_pools, f)