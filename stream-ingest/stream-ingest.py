'''import packages'''
import os, traceback, json, logging, base64, urllib, sys
import boto3
import dateutil.parser
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
'''in order to use Xray, add aws_xray_sdk to requirements.txt and npm install. then uncomment following lines'''
#from aws_xray_sdk.core import patch_all
#patch_all()

#set up LOGGER
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

#configure ssm
SSM_CLIENT = boto3.client('ssm')
SSM_CONFIG_PATH = '/'+ os.getenv('DBClusterName') +'/'

#database name
DATABASE_NAME = 'MyGame'

''' gets our configuration from parameter store'''
def load_config(ssm_parameter_path):
    try:
        param_details = SSM_CLIENT.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True
        )

        config_dict = {}
    
        # Loop through the returned parameters and populate config_dict
        if 'Parameters' in param_details and len(param_details.get('Parameters')) > 0:
            for param in param_details.get('Parameters'):
                param_key = param.get('Name').split("/")[-1]
                param_value = param.get('Value')
                config_dict[param_key] = param_value
            
            return config_dict
        else:
            raise Exception("SSM Parameter Store get parameter returned no results")
    except Exception as e:
        LOGGER.error(str(e))
        raise Exception("SSM Parameter Store get parameter failure")

''' creates our mongodb client connection'''
def mdbClient():
    config = load_config(SSM_CONFIG_PATH)
    
    if config is not None:
        mongo_hostname = config['hostname']
        mongo_user = urllib.quote_plus(config['username'])
        mongo_password = urllib.quote_plus(config['password'])
        
        try:
            LOGGER.info('Connecting to MongoDB {}'.format(mongo_hostname))
            connection_string = 'mongodb+srv://{}:{}@{}/{}'.format(mongo_user, mongo_password, mongo_hostname, DATABASE_NAME)
            LOGGER.info('connection string is: '+connection_string)
            
            try:
                mongo_client = MongoClient(connection_string)
                LOGGER.info('connected to mongodb')
                return mongo_client
            except Exception as e:
                LOGGER.error('Could not connect to {}: {}'.format(DATABASE_NAME, str(e)))
        except Exception as e:
            LOGGER.error(str(e))
    else:
        raise Exception("Unable to get MongoDB Config")

''' process records from Kinesis and store in MongoDB'''
def lambda_handler(event, context):
    mongo_client = mdbClient()
    try:
       # check to make sure we can connect to database.
       mongo_client.admin.command('ismaster')
    except ConnectionFailure:
       raise Exception("MongoDB connection failed")
    
    db = mongo_client[DATABASE_NAME]
    collection = db['Scores']
    records = []
    # Get data from events and put into MongoDB
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        LOGGER.info('Decoded payload: {}'.format(payload))
        
        #payload is of type string. converting to json
        json_payload = json.loads(payload)
        #timestamp needs to be proper ISODate type for mongodb
        json_payload["timestamp"] = dateutil.parser.parse(json_payload["timestamp"])

        try:
            result = collection.insert_one(json_payload)
        except Exception as e:
            LOGGER.error('Could not insert payload into scores collection: {}'.format(str(e)))
            sys.exit(1)
        
        LOGGER.info('Successfully inserted {} to scores collection'.format(json_payload))
        records.append(result)
    
    LOGGER.info('Successfully processed {} records'.format(len(records)))
    return len(records)
