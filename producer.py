from confluent_kafka import Producer
import inquirer

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n', (msg.topic(), msg.partition()))


def publish_on_topic(topic_to_publish, message):
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

    try:
        data = message
        p.produce(topic, data, callback=delivery_callback)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(p))
        p.poll(0)

    print('%% Waiting for %d deliveries\n' % len(p))
    p.flush()








questions = [
    inquirer.Text(name='name', message="Qual o nome do seu canal?"),
    inquirer.List('type_of_post',
                message="Qual o tipo da postagem {name} ? ",
                choices=['Vídeo', 'Live', 'Postagem na comunidade'],
            ),
    inquirer.Text(name='title', message='Qual o título da postagem?')
]

answers = inquirer.prompt(questions)
print(answers)

name = answers['name']
type_of_post = answers['type_of_post']
title = answers['title']
print(name, type_of_post, title)

publish_on_topic('esportes', 'Palmeiras venceu novamente')
publish_on_topic('games', 'Alanzoka jogando Overwatch')
publish_on_topic('noticias', 'Frente fria chegando')
