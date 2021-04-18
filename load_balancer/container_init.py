import os
import sys
import json , threading , time

def start_machine(module_name):

    build = f'docker build -t load_balancer ./load_balancer'
    os.system(build)
    cmd = f'docker run -dit  --network platform_net --name {module_name} load_balancer'
    os.system(cmd)
    print(f" {module_name} running")
    
if __name__ == '__main__':
	thread1 = threading.Thread(target=start_machine ,args=("load_balancer" ,))
	thread1.start()
