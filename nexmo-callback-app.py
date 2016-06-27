import ConfigParser
import phonenumbers
from flask import Flask
from flask import request
from kafka import KafkaProducer
from json import JSONEncoder


Config = ConfigParser.ConfigParser()
Config.read("./config.ini")

kafka_url = Config.get("Kafka", "servers")
publish_topic = Config.get("Nexmo-Callback", "kafka_topic")

producer = KafkaProducer(bootstrap_servers=kafka_url)

def send(msg):
    msisdn = request.args.get('msisdn')
    to = request.args.get('to')
    messageId = request.args.get('messageId')
    text = request.args.get('text')
    message_type = request.args.get('type')
    keyword = request.args.get('keyword')
    timestamp = request.args.get('message-timestamp')
    try: 
      number = phonenumbers.parse("+" + msisdn)
      message_object = {
          'msisdn' : msisdn,
          'to' : to,
          'messageId' : messageId,
          'text' : text.lower(),
          'message_type' : message_type,
          'keyword' : keyword,
          'timestamp' : timestamp,
          'country_code' : str(number.country_code),
          'country_area_code' : str(number.national_number)[:-7]
        }
    except Exception as e:
      message_object = {
          'msisdn' : msisdn,
          'to' : to,
          'messageId' : messageId,
          'text' : text.lower(),
          'message_type' : message_type,
          'keyword' : keyword,
          'timestamp' : timestamp,
          'country_code' : 'unknown',
          'country_area_code' : 'unknown'
        }
      print "error"
      
    jsonString = JSONEncoder().encode(message_object)
    producer.send(publish_topic, jsonString.encode('utf8'))
    return '<p>Sent %s to sms topic </p> \n' % str(text)

app = Flask(__name__)
app.add_url_rule('/<username>', 'send', (lambda username: send(request.args.get('text'))))

#@app.route("/send", methods=['GET', 'POST'])
#def hello2():
#    print request.user_agent
#    #return send(request.args.get('text'))
#    return "works"

@app.route("/", methods=['GET', 'POST'])
def hello():
    send(request.args.get('text'))
    #print request.user_agent
    return "Hello World!"

if __name__ == "__main__":
    #app.debug = True
    app.run(host= '0.0.0.0')
