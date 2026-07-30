                               APACHE KAFKA
							   ------------
	
 *. Kafka is a open source system.
 *. Kafka is communication b/w sender and reciver.
 *. It works on the publishing and subscribing model.                 R2

                                                                      R3
    ------------                    ------------                   ------------
	|          |        Publish     |          |   Subscribe      |           |
    | SENDER   |   -------------->  | KAFKA    |  --------------> | RECIVER   |
    |	       |                    |          |                  |           |
	------------					------------	               ------------ 
	
	
	
 *. Sender will publish the data by using kafka, Reciver will get the data based on the subscription model.
 *.Kafka having multiple recivers.
 
 When to use Apache kafka :
 *. Cab booking app : For suppose one user is trying to book cab, that request will reach mutilpe cab drivers.
    One driver acceted the request. The very next minute user will get notfication as one driver accepted your request.
	And very next some frequent time intervals you will get the data driver will reach you 10 minutes.So every time we need 
	to save data into database and get into the DB.But in real time we have multiple users and drivers, Db can't able to 
	load that much data.
*. Instead of using this apporch we have  MESSAGING SYSTEM.(Kafka)
*. Apache kafka will be publishing and subscribing messages model.
*.In driver application every movement of the driver will reach the one TOPIC of the Kafka (instance of kafka).
*.User application will subscribe the kafka ( inperticular TOPIC). Then user will get the information abot the driver in 
  every time intervals.
*. Apache kafka is DISTRUBUTED SYSTEM. It will not centralized data.It will distrubted along with mutiple servers,different
   clusters.It will load all data. It will handle the traffic very easily.
*. Example of food delivary apps, filght, Train tracking apps.
*. E-commerce,Banking,Travel,Life Insurence apache kafka will use.


Advantages of KAFKA:
*.High ThroughPut -> It was able to lot's of transactions.Distrubuted across multiple clusters.
*.Fault Tolerence -> It will handle our replicas as well(One leader is suffered to manage traffic another will take).
*.Scalable Application -> above two. And based on the situation it will generate the clusters.

ARCHITECTURE OF APACHE KAFKA:
*.Apache kafka will consists of 2 components.
  1.Kafka cluster
  2.Zookeper
*.Zookeper and kafka cluster is part of our kafka eco system.
*.Zookeper will handle the all senders and recivers.
*.Entire kafka cluster will handle by zookeper.
*.Kafka job is get the data and send the data, But how many senders and recivers are export to the zookeper.
*.Within the kafka cluster we have mutiple brokers.And within the brokers we have mutilple TOPICS.
*.All the data will store in the topics.
*.Within in the topics it self we have mutilple particiations.And data will store into the those particiations. Like p1 and p2.
*.Within in the topics we have OFFSETES as well. offsetes like arrays. How we are storing the data similarly we are stroing 
  topics as well.
*.RECIVER will recive the data accordingly off sets.(he can take latest one also).




                                     KAFKA ECOSYSTEM
                            |----------------------------------------
                            |                                       |
                            |           KAFKA CLUSTER               |
        OFFSETES            |   | ----------------------------|     |
                            |   |                             |     |
                            |   |  |--------------------|     |     |
                            |   |  |      TOPIC1        |     |     |
|-------------|             |   |  |  ----------        |     |     |                      |------------
|             |---------->  |   |  |  |   P1     |      |     |     |   -------------->    |            |
|   SENDER    |             |   |  |  ----------        |     |     |                      |RECIVER     |
|-------------|             |   |  |  -----------       |     |     |                      |-----------
                            |   |  |  |   P2     |      |     |     |
							|	|  |  ------------      |     |     |
                            |   |  ---------------------      |     |
                            |   |  |---------------------|    |     |
                            |   |  |     TOPIC2          |    |     |
                            |   |  -----------------------    |     |
							|	|------------------------------     |    BROKERS
                            |                                       |
                            |    |---------------------|            |
							|	 |  ZOOKEPER           |            |
							|	 |---------------------             |
                            |                                       |
                            |----------------------------------------


KAFKA INSTALLATION :
*. Go to the kafka installation in google. (Instead of am using Docker)
ae2ad2396d58784d376bc2bc1bee9a44653be3bafad29ddae8d321d6f97065aa							
*. To run kafka by using docker first we need to create docker image by using this cmd ->docker pull apache/kafka:3.7.2
*. To run kafka by sing docker we need to run below commonds in powershell

docker run -d ^
--name kafka ^
-p 9092:9092 ^
-e KAFKA_NODE_ID=1 ^
-e KAFKA_PROCESS_ROLES=broker,controller ^
-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 ^
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 ^
-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER ^
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT ^
-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 ^
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 ^
apache/kafka:3.7.2

*.To check which container and image and port is runing we need to use -> docker ps
*.To verify kafka logs -> docker logs -f kafka
*.Ctrl + C using this we can stop the viewing logs 
							
*.If we want to store the data then we need to create topics.For this 
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic employee-topic --bootstrap-server localhost:9092

*.To describe the topics -> docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic employee-topic --bootstrap-server localhost:9092							

*.TO create Producers (to publish)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic employee-topic --bootstrap-server localhost:9092

*.To create Consumers (Read the messages)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic employee-topic --from-beginning --bootstrap-server localhost:9092


To Build Application:
Producers : Need to create Topic by using apache kafka ->TopicBuilder.name().build();
 TO publish the data: Need to use KafkaTemplate<String,Object> -> kafkaTemplate.send(topicName,subject)
consumer : TO listen that message we need to use @KafkaListener(topics,groupId) 
In *.properties file we need to add these 
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer