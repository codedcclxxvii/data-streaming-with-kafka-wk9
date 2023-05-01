from confluent_kafka import Consumer, KafkaError

'''In this implementation, we create a Consumer object with the bootstrap.servers 
configuration pointing to our Kafka cluster, and subscribe to the mytopic topic.
We then use a loop to continuously poll for messages from Kafka, with a timeout of 1 second.
When we receive a message, we print it out to the console.'''

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['mytopic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition event')
        else:
            print('Error while receiving message:', msg.error())
    else:
        print('Received message:', msg.value().decode('utf-8'))
