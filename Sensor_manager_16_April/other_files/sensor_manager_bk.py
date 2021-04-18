from sensor_package import * 
import multiprocessing
#not using sensorlogs

collection_name = "sensors_registered"   #sensor_document
logging_collection = "logger_current"
logging_archive = "logger_archive"

stop_state = "stopped"
running_state = "running"

restart_command = "restart"
start_command = "start"
stop_command = "stop"

proceesses_running = []

app = Flask(__name__)

def service_count(serviceid,collection):
	
	count = 0
	for x in collection.find():
		if (x['serviceid'] == serviceid):
			count=count+1 
	return count

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

def do_logging(serviceid, state=stop_command, applicationname=None, temptopic=None, sensor_topic_id_name_list_for_all_sensors=None, data_rate=None, process_id=None):
	
	msg = "Service with serviceid "+serviceid+" is running."
	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	serviceid = str(serviceid)
	# stopping a service
	if(state==stop_command):

		
		count = service_count(serviceid, collection)
		msg = "Service with serviceid "+serviceid+" is stopped being served."

		if(count!=1): 
			print("Zhol in serviceid received")
			# if no service was found
			if(count==0):
				msg = "No service with serviceid "+serviceid+" present."
				return msg
			# if multiple services were found, stop them all, change their start to stopped
			else :
				msg = "Multiple occurences of service with serviceid "+serviceid+" found!, closing all."

		for x in collection.find():
			print(x)
			if ( (x["serviceid"]) == serviceid ):
				temptopic = x["temptopic"]
				process_id = x["process_id"]
				print("Closing the temporary topic : ", temptopic)
				print(close_process(process_id))
				myquery = { "serviceid": serviceid }
				newvalues = { "$set": { "state": stop_state } }
				collection.update_one(myquery, newvalues)

	# while startring  a service
	elif(state==start_command):

		logging_entry = {
			"serviceid" : serviceid,
			"state" : running_state,
			"applicationname" : applicationname,
			"temptopic" : temptopic,
			"sensor_topic_id_name_list_for_all_sensors" : sensor_topic_id_name_list_for_all_sensors,
			"data_rate" : data_rate,
			"process_id" : process_id
		}

		count = service_count(serviceid,collection)

		if (count>0):
			print("Kya re divyansh, do service same name? ")
			msg = "Service with serviceid "+serviceid+" was already running, restarting it."
			do_logging(serviceid, restart_command, applicationname, temptopic, sensor_topic_id_name_list_for_all_sensors, data_rate, process_id)
			return msg
		else:
			collection.insert_one(logging_entry)

	else: # restart_command

		logging_entry = {
			"serviceid" : serviceid,
			"state" : running_state,
			"applicationname" : applicationname,
			"temptopic" : temptopic,
			"sensor_topic_id_name_list_for_all_sensors" : sensor_topic_id_name_list_for_all_sensors,
			"data_rate" : data_rate,
			"process_id" : process_id
		}

		myquery = { "serviceid" : serviceid }
		newvalues = { "$set": logging_entry }
		collection.update_one(myquery, newvalues)

	return msg

def restart_services():

	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]

	d = list(collection.find())
	if(len(d) != 0):
		for i in d:
			state = i['state']
			if(state == running_state):
				sensor_topic_id_name_list_for_all_sensors = i['sensor_topic_id_name_list_for_all_sensors']
				applicationname = i['applicationname']
				serviceid = i['serviceid']
				temptopic = i['temptopic']
				data_rate = i['data_rate']
				process = multiprocessing.Process(target = bind_sensor_data_to_temptopic, args=(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, data_rate, applicationName, restart_command))
				process.start()
				print(f'started a service from logs {serviceid}')


def stop_service(serviceid):

	return do_logging(serviceid, stop_command)

def clear_logs():

	cluster = MongoClient(dburl)
	db = cluster[db_name]
	collection = db[logging_collection]
	collection.drop()
	return

def getSensorsForService(db, servicename, applicationName):

	l = db[applicationName]['algorithms'].keys() #list of services
	# print("list of srv :", l)
	listOfSensors = []
	for i in l:
		if i == servicename:
			listOfSensors = db[applicationName]['algorithms'][i]['sensors']
			break

	return listOfSensors

def resolveSensorQuery(query, latitude, longitude):

	cluster = MongoClient(dburl)
	db = cluster[db_name]
	col = db[collection_name]
	sensor_topic_id_name_list = []

	for sensor_num in range(len(query)):
		
		docs = col.find({})
		min_dist = sys.maxsize
		sensor_topic_id_name = None

		for i in docs:
			# print("sensorname, ",query[sensor_num])

			if i['sensor_name'] == query[sensor_num]:
				temp_lat = int(i['sensor_geolocation']['lat'])
				temp_long = int(i['sensor_geolocation']['long'])
				latitude = int(latitude)
				longitude = int(longitude)

				# print(abs(temp_lat - latitude)+abs(temp_long - longitude))

				if (abs(temp_lat - latitude)+abs(temp_long - longitude)) < min_dist:

					# print("distance is being updated kool")
					min_dist = abs(temp_lat - latitude) + abs(temp_long-longitude)
					sensor_topic_id_name = i['sensor_data_storage_details']['kafka']['broker_ip'] + "#" + i['sensor_data_storage_details']['kafka']['topic'] + "#" + i['sensor_id'] + "#" + i['sensor_name']
					print("sensor_topic_id_name : ",sensor_topic_id_name)

		sensor_topic_id_name_list.append(sensor_topic_id_name)

	return sensor_topic_id_name_list

'''
Step1: consumer reads/consumes all the data from the topic->topic (where sensors have been dumping the data).
Then we iterate through the consumer to get all the msgs that the consumer has read, and send those messages, as producer, to topic->temptopic. From this temptopic, the applications/etc can get the sensor data via sensor mgr
'''

def dump_data(ip, topic, serviceid, temptopic, data_rate, sensor_id, sensor_name):

	consumer = KafkaConsumer(str(topic), bootstrap_servers=[ip], auto_offset_reset = "latest",group_id=smgid )
	# print("Consumer hii print kara diey : ", consumer)
	# print("Consumer consumed data from sensor topic: ", str(topic))
	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)

	for message in consumer:
		# print("message along with sensor id : ")
		value = message.value.decode('utf-8')
		msg = sensor_id + "#" + sensor_name + "#" + value
		# msg = {
		# 	"sensor_id" : sensor_id,
		# 	"value" : value
		# }
		# print(msg)
		producer.send(temptopic, msg)
		producer.flush()
		time.sleep(int(data_rate))

def listen_action_manager():

	consumer = KafkaConsumer(str(action_manager_topic), bootstrap_servers=[KAFKA_PLATFORM_IP], auto_offset_reset = "earliest",group_id=smgid)
	for message in consumer:
		print("Take below Action : ")
		value = message.value.decode('utf-8')
		print(value)

def bind_sensor_data_to_temptopic(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, data_rate, applicationName, state):
	
	pid = os.getpid()
	proceesses_running.append(pid)
	# t = threading.Thread(target=do_logging, args=(serviceid, state, applicationName, temptopic, sensor_topic_id_name_list_for_all_sensors, data_rate, pid,))
	# t.start()
	msg = do_logging(serviceid, state, applicationName, temptopic, sensor_topic_id_name_list_for_all_sensors, data_rate, pid)
	print("logging done : ",msg)

	for i in range(len(sensor_topic_id_name_list_for_all_sensors)):
		ip, topic, sensor_id, sensor_name = sensor_topic_id_name_list_for_all_sensors[i].split('#')

		# print("ip and topic are :")
		# print(ip+" "+topic)

		# per sensor 
		t = threading.Thread(target=dump_data, args=(ip, topic, serviceid, temptopic, data_rate[i], sensor_id, sensor_name,))
		t.start()

def listen_exit_command(main_pid):
	
	inp = input()
	if(inp=="exit()"):
		clear_logs()
		for p in proceesses_running:
			close_process(p)
		# add_to_archive()
		print("Cleared logs.")
		close_process(main_pid)


@app.route('/stopSensormanager', methods = ['GET','POST'])
def stopSensorManagerUtil():

	data = request.get_json(force=True)
	serviceid = data['serviceid']
	msg = stop_service(serviceid)
	res = {
		'msg' : msg
	}
	return jsonify(res)


@app.route('/sensormanager', methods = ['GET','POST'])
def sensorManagerUtil():

	data = request.get_json(force=True)
	userid = data['username']
	applicationName = data['applicationname']
	servicename = data['servicename']
	serviceid = data['serviceid']
	d = data['config_file']

	#still confused about the lat and longitude
	latitude = data['latitude']
	longitude = data['longitude']

	#list of all sensors used for this service
	listOfSensorsWithDetails = getSensorsForService(d, servicename, applicationName)

	query = []
	data_rate = []

	# processing is not present in config file - neither in  sensor.json file
	# print("listOfSensorsWithDetails : ",listOfSensorsWithDetails)
	for i in listOfSensorsWithDetails:
		data_rate.append(listOfSensorsWithDetails[i]['processing']['data_rate'])
		query.append(listOfSensorsWithDetails[i]['sensor_name'])
	# print("query : ",query)   

	# query will have all the sensor names
	sensor_topic_id_name_list_for_all_sensors = resolveSensorQuery(query, latitude, longitude)
	# print("sensor_topic_id_name_list_for_all_sensors", sensor_topic_id_name_list_for_all_sensors)

	temptopic = serviceid + str(random.randrange(0, 1000))

	# per service
	# t = threading.Thread(target = bind_sensor_data_to_temptopic, args=(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, data_rate,))
	# t.start()
	process = multiprocessing.Process(target = bind_sensor_data_to_temptopic, args=(sensor_topic_id_name_list_for_all_sensors, serviceid, temptopic, data_rate, applicationName, start_command,))
	process.start()

	print(f'temptopic of serviceid {serviceid} is {temptopic}')
	res = { 
		'temporary_topic' : temptopic,
	}
	return jsonify(res)

if __name__ == '__main__':
	
	# start listening to the action manager
	main_pid = os.getpid()
	t1 = threading.Thread(target = listen_action_manager, args=())
	t1.start()
	t2 = threading.Thread(target = listen_exit_command, args=(main_pid,))
	t2.start()

	# if in case of sudden shutdown, check logs and restart services and topics
	# restart_services()

	# run apis
	app.run(host="0.0.0.0",port=manager_port)







