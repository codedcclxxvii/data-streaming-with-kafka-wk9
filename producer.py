from confluent_kafka import Producer
import json

'''In this implementation, we create a Producer object with the bootstrap.servers configuration pointing to our Kafka cluster.
We then define a send_to_kafka function that takes in a topic and data parameter, 
and sends the data to Kafka by producing a message to the specified topic.
'''
p = Producer({'bootstrap.servers': 'localhost:9092'})

def send_to_kafka(topic, data):
    try:
        p.produce(topic, key='data', value=json.dumps(data))
        p.flush()
    except Exception as e:
        print('Exception while sending data to kafka', e)
