
conda deactivate 
(ignore)

Simulators :

python3 start_sensors.py



sensor_codes :


python3 sensor_catalogue_registration.py

7071/addSensorType

Input : test_jsons/sensor_catalogue.json

7071/getAllSensorTypes



python3 sensor_instance_registation.py


7072/sensorRegistration

Input : test_jsons/sensor_instance.json

7072/getAllSensors



python3 sensor_manager.py

5050/sensorManagerStartService

Input : test_jsons/app_config.json

5050/stopService



Input : {sensor_id}


Mongo db cluster login detais:

Username : apurva.jadhav@students.iiit.ac.in
Pass : apuarushr123


Simulators :

python3 action_manager.py

python3 application_consumer.py



Start Kafka :



cd /Users/apurvajadhav/Desktop/ias_installs/kafka_2.13-2.7.0

bin/zookeeper-server-start.sh config/zookeeper.properties

JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties

cd /Users/apurvajadhav/Desktop/ias_installs/CMAK/target/universal/cmak-3.0.0.5

bin/cmak -Dconfig.file=conf/application.conf -Dhttp.port=8080








