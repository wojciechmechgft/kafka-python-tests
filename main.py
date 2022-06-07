import boto3

sts_client = boto3.client('sts')


assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::939877931619:role/preprod-system-cross-account-zoral-role",
    RoleSessionName="AssumeRoleSession"
)

credentials=assumed_role_object['Credentials']

print(credentials)