# pip3 install boto3
import boto3
# pip3 install git+https://github.com/mattoberle/kafka-python.git@feature/2232-AWS_MSK_IAM
# from kafka import KafkaConsumer

from confluent_kafka import Consumer



def print_debug(object, desc):
    print("--- {} ---------------------------------".format(desc))
    print(object)
    print()


sts_client = boto3.client('sts')


assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::939877931619:role/preprod-system-cross-account-zoral-role",
    RoleSessionName="AssumeRoleSession"
)

caller_identity = sts_client.get_caller_identity()

print_debug(caller_identity, "STS caller identity")
print_debug(assumed_role_object, "Assumed role object")

kafka_client = boto3.client('kafka', 
    region_name='ap-southeast-1',
    aws_access_key_id=assumed_role_object['Credentials'].get('AccessKeyId'),
    aws_secret_access_key=assumed_role_object['Credentials'].get('SecretAccessKey'),
    aws_session_token=assumed_role_object['Credentials'].get('SessionToken')
)

kafka_clusters_list = kafka_client.list_clusters()
print_debug(kafka_clusters_list, "Kafka clusters")


bootstrap_brokers = kafka_client.get_bootstrap_brokers(
    ClusterArn=kafka_clusters_list['ClusterInfoList'][0].get("ClusterArn")
)

print_debug(bootstrap_brokers, "Bootstrap brokers")
bootstrap_brokers_iam = bootstrap_brokers.get('BootstrapBrokerStringSaslIam')
print_debug(bootstrap_brokers_iam, "Bootstrap brokers: BootstrapBrokerStringSaslIam")

# https://github.com/dpkp/kafka-python/pull/2255

conf = {
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'AWS_MSK_IAM',
    "sasl.username": assumed_role_object['Credentials'].get('AccessKeyId'),
    "sasl.password": assumed_role_object['Credentials'].get('SecretAccessKey'),
    'bootstrap.servers': bootstrap_brokers_iam,
    'group.id': 'tests',
}

c = Consumer(conf)
c.subscribe(['financing-transfer-service.repayment.snapshot.v1'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()

# consumer = KafkaConsumer('financing-transfer-service.repayment.snapshot.v1',
#     # group_id='my_favorite_group',
#     # security_protocol='SASL_SSL',
#     # sasl_mechanism='AWS_MSK_IAM',
#     bootstrap_servers=bootstrap_brokers_iam
# )

# for msg in consumer:
#     print (msg)

