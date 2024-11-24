import boto3
from constants import REGION_NAME

# Clients with optional dependency injection
def get_boto3_client(service_name, region=REGION_NAME, mock_client=None):
    return mock_client or boto3.client(service_name, region_name=region)

cloudwatch_client = get_boto3_client("cloudwatch")
s3_client = get_boto3_client("s3")
secrets_client = get_boto3_client("secretsmanager")
sns_client = get_boto3_client("sns")
ssm_client = get_boto3_client("ssm", region="ap-southeast-2")
