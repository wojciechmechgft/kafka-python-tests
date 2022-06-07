import boto3
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

# https://github.com/confluentinc/confluent-kafka-python
# https://blog.dataminded.com/aws-msk-secure-python-kafka-client-1d25dae39207

consumer_conf = {
    'bootstrap.servers': bootstrap_brokers_iam,
    'group.id': 'python-tests'
    # 'security.protocol': 'SASL_SSL'
}

consumer = Consumer(consumer_conf)


consumer.subscribe(['financing-transfer-service.repayment.snapshot.v1'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

consumer.close()