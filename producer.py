from confluent_kafka import Producer
import inquirer

def delivery_callback(err, msg):
    if err:
        print(f'Message failed delivery: {err}\n')
    else:
        print(f'Message delivered to {msg.topic()} on partition {msg.partition()}\n')


def publish_on_topic(channel_name, topic_to_publish, message):
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
        data = ""
        if topic_to_publish == 'Video':
            data = channel_name + " postou o vídeo: " + message
        elif topic_to_publish == 'Live':
            data = channel_name + " iniciou uma transmissao ao vivo: " + message
        else:
            data = channel_name + " postou na comunidade: " + message


        p.produce(topic, data, callback=delivery_callback)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(p))
        p.poll(0)

    print(f'%% Waiting for %d deliveries\n' % len(p))
    p.flush()


name = input('Qual o nome do seu canal? ')

while True:
    questions = [
        inquirer.List('type_of_post',
                    message="Qual o tipo da postagem? ",
                    choices=['Video', 'Live', 'Post'],
                ),
        inquirer.Text(name='title', message='Qual o título da postagem?')
    ]

    answers = inquirer.prompt(questions)
    print(answers)

    type_of_post = answers['type_of_post']
    title = answers['title']
    print(name, type_of_post, title)

    publish_on_topic(name, type_of_post, title)
