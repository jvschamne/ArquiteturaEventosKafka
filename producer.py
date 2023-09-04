from confluent_kafka import Producer
def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n', (msg.topic(), msg.partition()))


def createTopic(topic_to_publish, message):
    topic = topic_to_publish
    bootstrapServers = 'pkc-ldjyd.southamerica-east1.gcp.confluent.cloud:9092'
    conf = {
        'bootstrap.servers': bootstrapServers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'HXE3A5JN2WIKIXOV',
        'sasl.password': 'n7sXmBPAVSp2e8BWG6vk37RxiJvCA8ZjS8fOLO/161m/i6H35KX8fHRi4j2Z96b4'
    }

    p = Producer(conf)

    messages = ['asd', 'asdasd', 'asd', 'asdasd', 'asd', 'asdasd', 'asd', 'asdasd']

    for m in messages:
        try:
                data = m
                p.produce(topic, data, callback=delivery_callback)
        except BufferError as e:
                print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(p))
                p.poll(0)


    print('%% Waiting for %d deliveries\n' % len(p))
    p.flush()


createTopic('games', 'Alanzoka jogando Overwatch')
createTopic('noticias', 'Frente fria chegando')