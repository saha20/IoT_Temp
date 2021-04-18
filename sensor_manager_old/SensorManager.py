import os , json , time , threading , smtplib
from kafka import KafkaProducer , KafkaConsumer
from json import loads
from json import dumps
from time import sleep


def json_deserializer(data):
    return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")



def getKafkaIP():
	# with  open ('ip_port.json', "r") as f:
	#     data = json.load(f)
	# kafka_platform_ip = data['allocatedPool']['Kafka']
	kafka_platform_ip = 'kafka:9092'
	return kafka_platform_ip

def get_input_from_AM():
	# listen_data = consume
	consumer = KafkaConsumer('actionManager_to_sensorManager',
	 bootstrap_servers = ['kafka:9092'],
	 auto_offset_reset = 'latest',
	 enable_auto_commit = True,
	 group_id = 'my-group',
	 value_deserializer=lambda x: loads(x.decode('utf-8')))
	 # value_deserializer = json_deserializer)
	
	for message in consumer:
		message = message.value
		# collection.insert_one(message)
		# print('{} added to {}'.format(message, collection))
		print( "Printing from get_input_from_AM")
		print(message)


def heartBeat():
	kafka_platform_ip = getKafkaIP()
	producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
	while True:
		t = time.localtime()
		current_time = int (time.strftime("%H%M%S", t))
		# print(current_time)

		data = {"module" : "Sensor_Manager" , "ts" : current_time , "Status" : 1   }
		producer.send("HeartBeat", data)
		time.sleep(5)



if __name__ == '__main__':
	thread1 = threading.Thread(target = heartBeat)
	thread1.start()
	get_input_from_AM()