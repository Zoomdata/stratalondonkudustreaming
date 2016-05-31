import ConfigParser
from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime, timedelta
from json import JSONDecoder, JSONEncoder
import thread
import time
import sys

Config = ConfigParser.ConfigParser()
Config.read("./config.ini")

kafka_url = Config.get("Kafka", "servers")
strata_sms = Config.get("Nexmo-Callback", "kafka_topic")
expanded_sms = Config.get("Redirect", "producer_kafka_topic")


producer = KafkaProducer(bootstrap_servers=kafka_url,batch_size=128000)
with open('fruits') as f:
  d = dict([x.rstrip(),1] for x in f)

def send_msgs(msg, size):
    for _ in range(size):
        producer.send(expanded_sms, msg.encode('utf-8'))

while True:
    consumer = KafkaConsumer(strata_sms
                             ,bootstrap_servers=kafka_url
                             #,auto_offset_reset='earliest'
                             )
    for msg in consumer:
        json_text = JSONDecoder().decode(msg.value)
        json_text['text']
        tokens = str(json_text['text']).split()
        #tokens = msg.value.split()
        try:
            size = int(tokens[1])
            size = min([size,10000])
            value = tokens[0].title().lower()
            value = value.title()
            json_text['text'] = value
            jsonString = JSONEncoder().encode(json_text)
            if value in d:
                new_msg = jsonString
                thread.start_new_thread(send_msgs, (new_msg, size))
        except:
            print "Unexpected error"
            #sys.exit(1)
