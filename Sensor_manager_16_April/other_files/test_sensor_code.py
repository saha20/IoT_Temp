from sensor_package import * 
import multiprocessing

#not using sensorlogs
data = ["a","b","c","d","e","f"]
test_port=12321
app = Flask(__name__)

def topic_create(data_rate,sensor_number):

	producer = KafkaProducer(bootstrap_servers=[KAFKA_PLATFORM_IP],value_serializer=json_serializer)
	# for msg in data:
	temptopic = "temptopic_"+str(sensor_number)
	count = 1
	while True:
		# msg = msg+"#"+str(sensor_number)
		msg = str(count)+"#"+str(sensor_number)
		producer.send(temptopic, msg)
		producer.flush()
		time.sleep(int(data_rate))

def thread_create_for_each_sensor():
	
	r = [1,2]
	print(os.getpid())
	for i in r:
		t = threading.Thread(target=topic_create, args=(5,i,))
		t.start()

def thread_create_for_each_sensor2():
	
	r = [3,4]
	print(os.getpid())
	for i in r:
		t = threading.Thread(target=topic_create, args=(5,i,))
		t.start()

@app.route('/testSM', methods = ['GET','POST'])
def testSMutil():

	process = multiprocessing.Process(target=thread_create_for_each_sensor)
	process.start()
	res = {"pid" : "data"}
	return jsonify(res)

@app.route('/testSM2', methods = ['GET','POST'])
def testSMutil2():

	process = multiprocessing.Process(target=thread_create_for_each_sensor2)
	process.start()
	res = {"pid" : "data"}
	return jsonify(res)

@app.route('/testkill', methods = ['GET','POST'])
def testkillutil():

	data = request.get_json(force=True)
	pid = data["pid"]
	to_kill=True
	msg = "Process killed with pid "+pid
	try:
	    os.kill(int(pid), 0)
	except OSError:
	    to_kill = False
	    msg = "no process with pid "+pid
	if(to_kill) : 
		command = "kill -9 "+pid
		os.system(command)


	res = {"msg" : msg}
	return jsonify(res) 

if __name__ == '__main__':
	app.run(host="0.0.0.0",port=test_port)





