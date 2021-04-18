import os
import json , threading , time


def start_machine(ip_port , module_name):

    build = f'docker build -t {module_name} ./{module_name}'
    os.system(build)



    ip , port = ip_port.split(':')
    cmd = f'docker run -d  --name {module_name} --net=dbz {module_name}'
    try : 
        os.system(cmd)
        print(f" {module_name} running on address {ip_port}")
    except : 
        print(f" {module_name} Failed on address {ip_port}")
    

# read pool file
with  open ('ip_port.json', "r") as f:
    data = json.load(f)
# print(data)
# for mac in data:
#     print(mac)

# modules = ["producer" , "consumer"]
modules = ["heart_beat" , "worker_node" , "action_manager" , "sensor_manager" ]
count = -1
for m in  data["freePool"]["cluster1"]:
    machine = m
    print(f'{machine} selected .......')
    thread1 = threading.Thread(target=start_machine ,args=(machine , modules[count]  ,))
    thread1.start()
    # start_machine(machine , modules[count] )
    count += 1
    if count >= 3 : break



#Create deployer
#



#create run-time mapping json.