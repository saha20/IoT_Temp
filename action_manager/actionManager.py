import os , json , time , threading , smtplib
from kafka import KafkaProducer , KafkaConsumer
from json import loads

kafka_platform_ip = None
sms , email , sms_email = 1 , 2 , 3     


url = "https://www.fast2sms.com/dev/bulk"
  
my_data = {
    'sender_id': 'FSTSMS', 
    'message': '', 
    'language': 'english',
    'route': 'p',
    'numbers': ''    
}
  

headers = {
    'authorization': 'Oad6mcfFgAjsKY9JnHLrGMpqvhy2zQB3okXE0UITVRl4Wb8S7PdO6UsTLwGoVDqW4Ylnpc785MZFAvXg',
    'Content-Type': "application/x-www-form-urlencoded",
    'Cache-Control': "no-cache"
}

def sendSms(receiver , msg):
	# my_data['numbers'] = receiver
	# my_data['message'] = msg
	# response = requests.request("POST",
 #                            url,
 #                            data = my_data,
 #                            headers = headers)
	# # load json data from source
	# returned_msg = json.loads(response.text)
	# print(returned_msg['message'])
    print("received msg : ", msg)



def sendMail(receiver , msg  ):
    print("inside send mail")
    print(msg)
    sender = 'ias.platform.app@gmail.com'
    password = 'Ias@Platfrom'
    smtpserver = smtplib.SMTP("smtp.gmail.com",587)
    smtpserver.starttls()
    smtpserver.login(sender,password)
	
    smtpserver.sendmail(sender,receiver,msg)
    print('sent')
    smtpserver.close()

def getKafkaIP():
    # with  open ('ip_port.json', "r") as f:
    #     data = json.load(f)
    # kafka_platform_ip = data['allocatedPool']['Kafka']
    kafka_platform_ip = 'kafka:9092'
    return kafka_platform_ip


def json_deserializer(data):
    return json.dumps(data).decode('utf-8')

def json_serializer(data):
	return json.dumps(data).encode("utf-8")

def notifySensorManager(data_SM):
    # producer fro SM :
    kafka_platform_ip = getKafkaIP()

    producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
    # data = {"sensor_manager" : data_SM}
    for data in data_SM:
        producer.send("actionManager_to_sensorManager", data)



def notifyUsers(data_for_users):
    sz = len(data_for_users)

    for user_addr in data_for_users:
        if '@' in user_addr:
            sendMail(user_addr, "hi... this is for testing")
        else:
            sendSms(user_addr , "demo msg")
            






def listenForInstruction():
    # listen_data = consume
    consumer = KafkaConsumer('action_manager',
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
        print( "Printing from listenForInstruction")
        print(message)
        listen_data = message["action_center"]
        to_sensor_manager = listen_data["sensor_manager"]
        to_notify_users = listen_data["notify_users"]
        to_user_display = listen_data["user_display"]
        subject = to_user_display



        if len(to_sensor_manager) > 0:
            notifySensorManager(to_sensor_manager)

        notifyUsers(to_notify_users)
        time.sleep(10)
    


def heartBeat():
    kafka_platform_ip = getKafkaIP()
    producer = KafkaProducer(bootstrap_servers=[kafka_platform_ip],value_serializer =json_serializer)
    while True:
        t = time.localtime()
        current_time = int (time.strftime("%H%M%S", t))
        # print(current_time)

        data = {"module" : "Action_Manager" , "ts" : current_time , "Status" : 1   }
        producer.send("HeartBeat", data)
        time.sleep(5)



if __name__ == "__main__":
    thread1 = threading.Thread(target = heartBeat)
    # listenForInstruction()
    thread1.start()
    listenForInstruction()


