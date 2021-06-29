# Edge Program
# Each data point should be read after every 60-sec delay and publish to the cloud(live data)
# If the server returns failure or server is stopped the data point is buffered locally(it now becomes buffered data). 
# Publish all the buffered data after every 5 seconds. 
# While cleaning the buffered data the live data should not be stopped, i.e. the program should be properly multithreaded.
# Example: 1st & 2nd minute, 1st & 2nd data points published successfully, 3rd & 4th minute, 3rd & 4th data points failed and got buffered, 
# 5th min 5th datapoint and all the buffered data published.
# Edge Program - Expose a function to get the count of successfully transmitted and buffered data at any point in time.


# 1.Read Csv every 60sec
# 2.Read=> Publish to server over mqtt
# 3. Check whether data sent or buffered depending on the response and increment appropriete counter
# 4. If buffered, queue it and send it along with live data
# 5. 

import csv
import string
import threading
import os
import time
import paho.mqtt.client as mqtt
from queue import Queue
q=Queue()
messages=[]

i=0
dataSent=0
dataBuffered=0
totalPublishedMsgs=0
broker_address="5.196.95.208" 
print("creating new instance")
client = mqtt.Client("P1") #create new instance
print("connecting to broker")
client.connect(broker_address) #connect to broker
client.subscribe("tempCloud/sensor/status")
topic="tempCloud/sensor/status"



def nextDataPoint(i):
    # global i
    j=0
    with open('addonsZenatix/dataset.csv') as dataPoints:
            data = csv.reader(dataPoints)
            for row in data:
                if(j==i):
                    timestamp=row[0]
                    value=row[1]
                    sensor=row[2]
                j=j+1
            dataPoints.close()
            
            return {timestamp, value, sensor}

def sendNextData(dataPoint):
    
    client.connect(broker_address) #connect to broker
    print("Publishing message to topic","tempCloud/sensor/")
    msg= str(dataPoint)
    client.publish("tempCloud/sensor/",msg)
    return checkDataSuccess


def checkDataSuccess(client1, userdata, message):
    #global messages
    m="message received  "  ,str(message.payload.decode("utf-8"))
    check = str(message.payload.decode("utf-8"))
    messages.append(m)#put messages in list
    q.put(m) #put messages on queue
    print("message received  ",m)
    if check=="Success":
        return True
    elif check=="Fail":
        return False
    else:
        return "Error"


def on_publish (client, userdata, mid):
    global messages
    global totalPublishedMsgs
    m="on publish callback mid "+str(mid)
    totalPublishedMsgs=totalPublishedMsgs+1

def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        client.subscribe(topic)
    else:
        print("Bad connection Returned code=",rc)
        client.loop_stop()  
def on_disconnect(client, userdata, rc):
   pass

    

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message=checkDataSuccess        #attach function to callback
client.on_publish =on_publish        #attach function to callback
print("Edge program started")
try:
    while(1):
        datapoint=nextDataPoint(i)
        print(nextDataPoint(i))
        status=sendNextData(datapoint)
        i=i+1
        if(status):
            dataSent=dataSent+1
        elif(not sendNextData()):
            dataBuffered=dataBuffered+1

        print("Data Sent = "+str(dataSent))
        print("Data buffered = "+str(dataBuffered))

        time.sleep(60)

except KeyboardInterrupt:
    print("interrupted  by keyboard")



