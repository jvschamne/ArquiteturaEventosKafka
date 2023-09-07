from confluent_kafka import Consumer
import inquirer

def createConsumer(topics_of_interest):
    topics = topics_of_interest
    print('CONFIGURATIONS')

    conf = {
    'bootstrap.servers': 'pkc-ldjyd.southamerica-east1.gcp.confluent.cloud:9092',
    'group.id': 'grupo_id',  
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'HXE3A5JN2WIKIXOV',
    'sasl.password': 'n7sXmBPAVSp2e8BWG6vk37RxiJvCA8ZjS8fOLO/161m/i6H35KX8fHRi4j2Z96b4'
    }

    print('CREATE CONSUMER')
    c = Consumer(conf)
    c.subscribe(topics)
    print('Aguardando mensagens')
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # mensagem de erro
                print('Msg error:', msg.error().str() )
            else:
                # print com a mensagem do tópico
                print('Mensagem tópico')
                print(msg.value().decode('utf-8'))


    except KeyboardInterrupt:
        print('Aborted by user\n')
        # Close down consumer to commit final offsets.
    c.close()


questions = [
    inquirer.Text(name='name', message="Qual o seu nome?"),
    inquirer.Checkbox('topics',
                message="Em quais tópicos você tem interesse {name} ? ",
                choices=['Video', 'Live', 'Post'],
            ),
]

answers = inquirer.prompt(questions)
print(answers)

topics = answers['topics']

createConsumer(topics)
