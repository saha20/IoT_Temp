import os
import sys
import json , threading , time
import paramiko

def start_machine(machine_names):

    build = f'docker build -t service_host ./service_host'
    os.system(build)
    for machine_name in machine_names:
    	cmd = f'docker run -dit  --network platform_net --name {machine_name} service_host'
    	try : 
    		os.system(cmd)
    		cmd='docker inspect -f \'{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' '+machine_name
    		host=os.popen(cmd).read()
    		print(host.strip())
    		print(f" {machine_name} running")
    		ssh_client=paramiko.SSHClient()
    		ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    		ssh_client.connect(hostname=host.strip(),port = 22,username='root',password='root')
    		print(f" {machine_name} ssh running")
    		transport = ssh_client.get_transport()
    		channel = transport.open_session()	
    		pty = channel.get_pty()
    		shell = ssh_client.invoke_shell()
    		shell.send("cd ../work_dir/; nohup python3 container_server.py > /dev/null 2>&1 &\n")
    		time.sleep(2)
    		
    	except Exception as e: 
    		print(f" {machine_name} Failed")
    		print(str(e))
    
if __name__ == '__main__':
	thread1 = threading.Thread(target=start_machine ,args=(sys.argv[1:] ,))
	thread1.start()
