
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import boto3
import logging
import time
import json
from datetime import datetime
from random import randint


host = "a1ggladwtvbsba-ats.iot.us-east-1.amazonaws.com" #replace this with your IoT host name
rootCAPath = "root-CA.crt" #replace this with your root CA certificate
certificatePath = "windturbine.cert.pem" #replace this with your device certificate
privateKeyPath = "windturbine.private.key" #replace this with your device private key
clientID = "basicPubSub"

print ("Activating turbines...\n")

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None

myAWSIoTMQTTClient = AWSIoTMQTTClient(clientID)

myAWSIoTMQTTClient.configureEndpoint(host, 8883)

myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)


# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

print ("connecting...")


# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()


print ("connected!")

anomaly_counter = 0

#Now we go in a loop and generate simulated telemetry to send over the AWS IoT Core
while True:
    #telemetry will be sent ever two seconds so we pause the program for 2 seconds in each loop
    time.sleep (1) 

    anomaly_counter = anomaly_counter+1
     
    timestamp = str (datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    turbine_id = randint(1,10)
    wind_speed = randint(40,85)
    RPM_blade = randint(25,85)
    oil_temperature = randint(10,50)
    oil_level = randint(1,25)
    vibration_frequency = randint(2,15)
    pressure = randint(30,85)
    wind_direction = randint(1,8)    
    temp = randint(70,85)
    humid = randint(30,70)

    location = str(randint(1,100))

    if anomaly_counter == 120:
        turbine_id = 1
        wind_speed = 1
        RPM_blade = 1
        oil_temperature = 1
        oil_level = 1
        vibration_frequency = 1
        pressure = 1
        wind_direction = 1   

        temp = 1
        humid = 1
        anomaly_counter = 0



    #randomly generate geo_location
    geo_location = '51.70015' + location + ', -0.5997986'
    print ("----------------------------------------------------------------------------------------------------------------")
    
    jpayload = {}

    #prepare json payload for IoT
    jpayload ['timestamp'] = timestamp
    jpayload ['geo_location'] = geo_location
    jpayload ['turbine_id'] = int(turbine_id)
    jpayload ['wind_speed'] = int(wind_speed)
    jpayload ['RPM_blade'] = int(RPM_blade)
    jpayload ['oil_temperature'] = int(oil_temperature)
    jpayload ['oil_level'] = int(oil_level)
    jpayload ['temperature'] = int(temp)
    jpayload ['humidity'] = int(humid)
    jpayload ['vibration_frequency'] = int(vibration_frequency)
    jpayload ['pressure'] = int(pressure)
    jpayload ['wind_direction'] = int(wind_direction)                

    json_data = json.dumps(jpayload)    

    print (json_data)
    print ("\n") 

    # now publish to the topic...
    myAWSIoTMQTTClient.publish("topic_1", json_data, 1)
       
 
        
