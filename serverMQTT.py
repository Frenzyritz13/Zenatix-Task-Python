# 1.Server Program - Expose a route on an HTTP server or a topic on MQTT broker which accepts data. 
# It should randomly give success or failure for the received data. 
# Each new data should append to a CSV file.

#1. Connect to mqtt
# 2. Get data on a topic
#3. Randomly send back success or failure with the received data
#4. Write to CSV the received data

# import paho.mqtt.subscribe as subscribe
import csv
import paho.mqtt.client as mqtt  #import the client1
import time
import random
from queue import Queue
q=Queue()
messages=[]
broker_address="5.196.95.208" 
print("creating new instance")
client = mqtt.Client("P1") #create new instance
print("connecting to broker")
client.connect(broker_address) #connect to broker
client.subscribe("tempCloud/sensor/")

def on_message_print(client, userdata, message):
    print("%s %s" % (message.topic, message.payload))

def on_message(client1, userdata, message):
    #global messages
    m="message received  "  ,str(message.payload.decode("utf-8"))
    msgReceived=str(message.payload.decode("utf-8"))
    messages.append(m)#put messages in list
    q.put(m) #put messages on queue
    print("message received  ",m)
    csvWriterFunction(msgReceived)
    mylist = ["Success", "Fail"]
    dataStatus=random.choice(mylist)
    client.publish("tempCloud/sensor/status",dataStatus)

def on_publish (client, userdata, mid):
    global messages
    m="on publish callback mid "+str(mid)

def csvWriterFunction(msgReceived):
    with open('final.csv', mode='a+', newline='') as csv_file:
        fieldnames = ['Timestamp', 'Value', 'Sensor']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writerow(msgReceived)
def on_connect(client, userdata, flags, rc):
    client.connected_flag=True
    #messages.append(m)
    #print(m)


client.on_connect= on_connect        #attach function to callback
client.on_message=on_message        #attach function to callback
client.on_publish =on_publish        #attach function to callback
#client1.on_subscribe =on_subscribe        #attach function to callback
time.sleep(1)
print("connecting to broker")
client.connect(broker_address)      #connect to broker
print("Server program started")
try:
    while(1):
        client.connect(broker_address)


        # time.sleep(10)

except KeyboardInterrupt:
    print("interrupted  by keyboard")


