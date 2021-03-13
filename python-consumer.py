from kafka import KafkaConsumer
from json import loads
import json
from mysql.connector import connect, Error
import mysql.connector
from datetime import datetime
from datetime import date

consumer = KafkaConsumer('new01', bootstrap_servers = ['localhost:9092'], value_deserializer = lambda m: json.loads(m))

DB_NAME = 'Exotel'

conn = connect(host='127.0.0.1', user = 'root', password = 'emc20703@SQL', port = '3306')
  
cursor = conn.cursor()

#to create DB
def create_database(cursor):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET utf8 COLLATE utf8_bin".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    create_database(cursor)
    print("Database {} created successfully.".format(DB_NAME))
    conn.database = DB_NAME

for msg in consumer:
    values = msg.value

    sid = values['sid']
    parentcallsid = values['Parentcallsid'] 
    datecreated = values['Datecreated'] 
    dateupdated = values['Dateupdated'] 
    accountsid = values['Accountsid'] 
    tono = values['To'] 
    fromno = values['From']  
    phonenumbersid = values['PhoneNumberSid'] 
    status = values['Status'] 
    starttime = values['StartTime'] 
    endtime = values['EndTime'] 
    duration = values['Duration']  
    price = values['Price'] 
    direction = values['Direction']
    answeredBy = values['AnsweredBy']
    forwardedFrom = values['ForwaredeFrom']  
    callerName = values['CallerName'] 
    uri = values['Uri'] 
    recordingUrl = values['RecordingUrl']

    cursor.execute("CREATE TABLE IF NOT EXISTS `call` (Sid varchar(64), AccountSid varchar(64) NOT NULL, `To` varchar(32) NOT NULL, `From` varchar(32) NOT NULL, PhoneNumberSid varchar(64) NOT NULL,`Status` enum('queued','ringing','in-progress','completed','failed','busy','no-answer') NOT NULL, `StartTime` datetime NOT NULL DEFAULT '1970-01-01 05:30:00', `EndTime` datetime NOT NULL DEFAULT '1970-01-01 05:30:00', `Duration` int(11) DEFAULT NULL, `Price` decimal(12,6) DEFAULT NULL, `Direction` enum('inbound','outbound-api','outbound-dial') DEFAULT NULL, AnsweredBy enum('human','machine') DEFAULT NULL, ForwardedFrom varchar(32) DEFAULT NULL, CallerName varchar(128) DEFAULT NULL, `Uri` varchar(128) DEFAULT NULL, `RecordingUrl` varchar(128) DEFAULT NULL, `DateCreated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, `DateUpdated` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (Sid,DateCreated))ENGINE=InnoDB;")
    conn.commit()

    args = (sid, accountsid, tono, fromno, phonenumbersid, status, starttime, endtime, int(duration), float(price), direction, answeredBy, forwardedFrom, callerName, uri, recordingUrl, datecreated, dateupdated)
    cursor.execute("INSERT INTO `call` (Sid, AccountSid, `To`, `From`, PhoneNumberSid, `Status`, `StartTime`, `EndTime`, `Duration`, `Price`, `Direction`, AnsweredBy, ForwardedFrom, CallerName, `Uri`, RecordingUrl, `DateCreated`, `DateUpdated`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ", args)
    conn.commit()
    
    #for testing the uploaded data in DB
    cursor.execute('SELECT Sid FROM `call` WHERE `Status` = %s ', ('completed', ))
    myresult = cursor.fetchall()
    print(myresult)


