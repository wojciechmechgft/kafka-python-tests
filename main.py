import boto3

def print_debug(object, desc):
    print("--- {} ---------------------------------".format(desc))
    print(object)
    print("-------------------------------------")


sts_client = boto3.client('sts')


assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::939877931619:role/preprod-system-cross-account-zoral-role",
    RoleSessionName="AssumeRoleSession"
)

caller_identity = sts_client.get_caller_identity()

print_debug(caller_identity, "STS caller identity")
print_debug(assumed_role_object, "Assumed role object")