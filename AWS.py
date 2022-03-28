from botocore.exceptions import ClientError
import boto3, json, base64, os, datetime
from os import environ

# region
serviceRegion = 'us-east-1'
profile_name = 'daybreak_dev'
session = boto3.session.Session(**({} if "AWS_EXECUTION_ENV" in environ else {'profile_name': profile_name}))

# db proxy details
DB = os.environ.get('DB_NAME', 'lighthouse_ods')
DB_ENDPOINT = os.environ.get('DB_HOST', 'lighthouseods.cxpucesrdf76.us-east-1.rds.amazonaws.com')
DB_PORT = '5432'
DB_USER = 'ods_rw'



def update_secret(secretname, secretvalue, regionname='us-east-1'):
    client = session.client(
        service_name='secretsmanager',
        region_name=regionname
    )
    secrets = client.list_secrets()
    secret = None
    for possiblesecret in secrets['SecretList']:
        if possiblesecret['Name'] == secretname:
            secret = possiblesecret
    if secret is not None:
        client.update_secret(
            SecretId=secret['ARN'],
            Description=secret['Description'],
            SecretString=json.dumps(secretvalue),
        )


def get_secret(secretname, regionname='us-east-1'):
    client = session.client(
        service_name='secretsmanager',
        region_name=regionname
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secretname
        )
    except ClientError as sme:
        print(sme)
        #raise sme
    else:
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        secret = None
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return json.loads(secret)


def pushtosqs(message, messagegroupid, regionname='us-east-1'):
    client = session.client(
        service_name='sqs',
        region_name=regionname
    )
    queue_url = 'https://sqs.us-east-1.amazonaws.com/937741081641/mercuria-integration.fifo'
    response = client.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=messagegroupid
    )
    return response['MessageId']


def uploadlogstos3(keyPrefix, srcDir='/tmp/', dstBucket='copperhill-integrations', regionname='us-east-1', timestamp=None):
    ts = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    if timestamp != None:
        ts = timestamp
    client = session.client(
        service_name='s3',
        region_name=regionname
    )
    for filename in os.listdir(srcDir):
        if filename.endswith('.csv'):
            data = open(srcDir+filename, 'rb')
            destinationname = keyPrefix + '/' + ts.replace('/', '').replace(':', '') + '_' + filename
            client.put_object(Body=data, Bucket=dstBucket, Key=destinationname)
            print('Uploaded https://s3.console.aws.amazon.com/s3/object/' + dstBucket + '/' + destinationname)
            os.remove(srcDir+filename)


def gets3filecontents(srcfilename, bucketname, region='us-east-1'):
    client = session.client(service_name='s3', region_name=region)
    s3response = client.get_object(Bucket=bucketname, Key=srcfilename)
    return s3response['Body'].read()


def get_ddb(tablename, jobName, regionname='us-east-1'):
    client = session.client(
        service_name='dynamodb',
        region_name=regionname
    )
    response = client.get_item(
        TableName=tablename,
        Key={'jobName': {'S': str(jobName)}}
    )
    return response['Item']['lastRun']['S']


def put_ddb(tablename, jobName, value, regionname='us-east-1'):
    client = session.client(
        service_name='dynamodb',
        region_name=regionname
    )
    response = client.put_item(
        TableName=tablename,
        Item={'jobName': {'S': str(jobName)}, 'lastRun': {'S': str(value)}}
    )
    return response['ResponseMetadata']['HTTPStatusCode']


def list_bucket(bucketname, prefix):
    s3 = session.client('s3')
    return s3.list_objects_v2(Bucket=bucketname, Prefix=prefix)['Contents']


def s3_get(bucketname, key, localpath):
    s3 = session.client('s3')
    s3.download_file(Bucket=bucketname, Key=key, Filename=localpath)


def s3_archive(srcbucket, srckey, dstbucket, dstkey):
    s3 = session.client('s3')
    copy = s3.copy_object(Bucket=dstbucket, CopySource=srcbucket+'/'+srckey, Key=dstkey)
    if 'ResponseMetadata' in copy:
        if 'HTTPStatusCode' in copy['ResponseMetadata'] and copy['ResponseMetadata']['HTTPStatusCode'] == 200:
            delete = s3.delete_object(Bucket=srcbucket, Key=srckey)


def check_path(bucketname, file_path):
    s3 = session.client('s3')
    result = s3.list_objects(Bucket=bucketname, Prefix=file_path )
    exists = False
    if 'Contents' in result:
        exists = True
    return exists


def bucket_exists(bucketname):
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucketname)

    if bucket.creation_date is not None:
        return True
    else:
        return False


def create_bucket(bucketname):
    s3 = session.resource('s3')
    s3.create_bucket(Bucket=bucketname)


def set_bucket_tag(bucketname, key, p_value):
    s3 = session.client('s3')
    response = s3.put_bucket_tagging(
        Bucket=bucketname,
        Tagging={
            'TagSet': [{'Key': key, 'Value': p_value}]
        }
    )


def put_in_object(binary_object, bucketname, keyname):
    s3 = session.resource('s3')
    object = s3.Object(bucketname, keyname)
    object.put(Body=binary_object)
    # s3.put_object(Body=binary_object, Bucket=bucketname, Key=keyname)

