import time , threading , json , os, sys
from time import sleep

from kafka import KafkaProducer


def getKafkaIP():
	# with  open ('ip_port.json', "r") as f:
	#     data = json.load(f)
	# kafka_platform_ip = data['allocatedPool']['Kafka']
	kafka_platform_ip = 'kafka:9092'
	return kafka_platform_ip

def sendData_to_ActionManager():
	# producer fro SM :
	kafka_platform_ip = getKafkaIP()

	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	sensor_id = 91
	data = 	{
			   "action_center":{
				  "user_display":"results",
				  "sensor_manager":[
					 {
						"sensor_id":sensor_id
					 },
					 {
						"command":"insert new threadss"
					 }
				  ],
				  "notify_users":[
					 "souptikmondal2014@gmail.com",
					 "7001275910",
					 "7980908816"
				  ]
			   }
			}

	producer.send("action_manager", data)





def json_deserializer(data):
	return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")


def heartBeat():
	kafka_platform_ip = getKafkaIP()
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		print(current_time)
		data = {"module" : "Worker_Node" , "ts" : current_time}
		producer.send('HeartBeat', value=data)
		sleep(5)
	 





if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	sendData_to_ActionManager()