#import statements
import random
import string
from datetime import datetime
from datetime import timedelta
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json

#creating dict for sending json dump
elem = {}
#count variable to check instance count on producers screen
count = 0

while True:
    #generating random data
    count += 1
    sid = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    parentcallsid = ''
    datecreated = str(datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S"))
    dateupdated = str(datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") +timedelta(seconds = 2))
    accountsid = 'Exotel'
    tono = ''.join(random.choice(string.digits) for _ in range(11))
    fromno = ''.join(random.choice(string.digits) for _ in range(11))
    phonenumbersid = ''.join(random.choice(string.digits) for _ in range(11))
    status = random.choice(['queued','ringing','in-progress','completed','failed','busy','no-answer'])
    starttime = str(datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S"))
    if status !='completed':
        price = str(0.0)
        duration = str(0)
        endtime = str(datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") + timedelta(seconds = int(duration)))
    else:        
        duration = str(random.randint(1,86400))
        endtime = str(datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S") + timedelta(seconds = int(duration)))
        price = str(1.500*(int(duration)/60))
    direction = random.choice(['inbound','outbound-api','outbound-dial'])
    answeredBy = random.choice(['human','machine'])
    forwardedFrom = ""
    callerName = ""
    uri = "/v1/Accounts//Calls/"+sid
    recordingUrl = "https://s3-ap-southeast-1.amazonaws.com/exotelrecordings//"+sid+".mp3"
    
    #loading dictionary with required data
    elem['sid'] = sid
    elem['Parentcallsid'] = parentcallsid
    elem['Datecreated'] = datecreated
    elem['Dateupdated'] = dateupdated
    elem['Accountsid'] = accountsid
    elem['To'] = tono
    elem['From'] = fromno
    elem['PhoneNumberSid'] = phonenumbersid
    elem['Status'] = status
    elem['StartTime'] = starttime
    elem['EndTime'] = endtime
    elem['Duration'] = duration
    elem['Price'] = price
    elem['Direction']= direction
    elem['AnsweredBy'] = answeredBy
    elem['ForwaredeFrom'] = forwardedFrom
    elem['CallerName'] = callerName
    elem['Uri'] = uri
    elem['RecordingUrl'] = recordingUrl

    #for checking how many instances of data have been send across Kafka
    print('Data sending instance', count, 'at', datetime.now())

    #initializing Kafka Producer
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    producer.send('new01', elem) #new01 is topic of Kafka
    producer.flush()
   
