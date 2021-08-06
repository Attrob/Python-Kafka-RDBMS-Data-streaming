# Python-Kafka-RDBMS-Data-Streaming
Task Name: Program to stream data from Kafka to relation database (MySQL)

Language Used: Python

Python Libraries Used: mysql.connector, kafka-python

**#Pre-requisites**
1. Python3.x 
2. Kafka
3. MySQL

**#Versions Used**
1. Python 3.7.9
2. Kafka 2.7.0 
3. MySQL 8.0

**#To install python libraries use the following commands**

pip install kafka-python

pip install MySQL-python

**#For Kafka Installation visit below URL for steps**

https://www.geeksforgeeks.org/how-to-install-and-run-apache-kafka-on-windows/#:~:text=Downloading%20and%20Installation&text=Step%201%3A%20Go%20to%20the,kafka%20folder%20and%20open%20zookeeper.

**#For MySQL visit below URL for installation steps**

https://www.youtube.com/watch?v=WuBcTJnIuzo


**#Commands to be executed before running Python scripts**

Go to directory where Kafka is installed and run the following commands

**#To run zookeeper - it runs on **localhost:2181****

...Kafka> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

**#To run Kafka Server - it runs on **localhost:9092****

...Kafka> .\bin\windows\kafka-server-start.bat .\config\server.properties

**#Create a kafka topic to use. Use this same topic name in Python (producer and consumer) codes**

...Kafka> .\bin\windows\kafka-topics.bat --create --zookeeper **localhost:2181** --replication-factor 1 --partitions 1 --topic **<topic name>**

**#Ports used by Kafka and MySQL**

Kafka Zookeeper = localhost:2181

Kafka Server = localhost:9092

MySQL server = localhost:3306

Run the python code uploaded once Kafka and MySQL is setup

Always run python-consumer.py first to initialize the consumer

Run python-producer.py next, to feed data to consumer

line 64-66 in python-consumer.py is to test whether the DB is populated with Data.

**Cheers!**
**Thankyou**
