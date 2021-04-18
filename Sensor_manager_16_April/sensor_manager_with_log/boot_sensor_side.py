# boot_sensor_side.py

import os
import threading
import multiprocessing

folder = ""

process_list = []
main_pid=None

def start_file(fname):
    print("fname :",fname)
    os.system("python3 {} &".format(fname))

def run_command(command):
    process_list.append(os.getpid())
	os.system("{}".format(command))

def close_process(pid):
    pid = str(pid)
    to_kill = True
    msg = "Process killed with pid "+pid
    try:
        os.kill(int(pid), 0)
    except OSError:
        to_kill = False
        msg = "No process with pid "+pid
    if(to_kill) : 
        command = "kill -9 "+pid
        os.system(command)
    return msg

def listen_exit():
    inp = input()
    if(inp=="exit()"):
        for p in process_list:
            close_process(p)
    close_process(main_pid)

if __name__ == '__main__':

	# origwd = os.getcwd()

	# os.chdir(kafka_folder)
	# command = ["bin/zookeeper-server-start.sh config/zookeeper.properties","JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties", ]
 #    for c in command:
 #        threading.Thread(target=run_command, args=(c,)).start()
 #        print("done !")

 #    os.chdir(kafka_manager_folder)
 #    manager_command = "bin/cmak -Dconfig.file=conf/application.conf -Dhttp.port=8080"
 #    threading.Thread(target=run_command, args=(manager_command,)).start()
 #    print("done !")

 #    os.chdir(origwd)

    main_pid = os.getpid()
    print("Enter exit() command to stop.")
    threading.Thread(target=listen_exit).start()

    file_names = ["sensor_catalogue_registration.py","sensor_instance_registation.py","start_sensors.py","sensor_manager.py"]
    
    for fname in file_names:
        target_file = os.path.join(folder,fname)
        multiprocessing.Process(target=start_file, args=(target_file,)).start()
        print("{} launched !".format(fname))

