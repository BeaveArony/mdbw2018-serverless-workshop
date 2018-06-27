'''import packages'''
import os, logging, urllib, sys, datetime, json
import boto3
import geojson
from geojson import Feature, Point, FeatureCollection
from bson import json_util
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
'''in order to use Xray, add aws_xray_sdk to requirements.txt and npm install.
then uncomment following lines'''
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

''' create the geoJson from our mongodb results'''
def getGeobject(mongoResults):
    geoResults = []
    jsonData = json.loads(mongoResults)
    for gameData in jsonData:
      my_feature = Feature(geometry=Point((float(gameData['longitude']), float(gameData['latitude']))), properties={"score": str(gameData['score'])})
      geoResults.append(my_feature)

    feature_collection = FeatureCollection(geoResults)
    return feature_collection

''' required to properly output to API Gateway'''
def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else res,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': "*"
        },
    }

''' Gets the scores from MongoDB based on a seconds time converted to minutes'''
def getScoresByTime(event, context):
    mongo_client = mdbClient()
    try:
       # check to make sure we can connect to database.
        mongo_client.admin.command('ismaster')
    except ConnectionFailure:
        raise Exception("MongoDB connection failed")

    db = mongo_client[DATABASE_NAME]
    collection = db['Scores']
    minutes = ''

    #see how many minutes back to go
    try:
        if int(event['pathParameters']['seconds'])%60 == 0:
            minutes = int(event['pathParameters']['seconds'])/60
        else:
            minutes = 1
    except:
        minutes = 1

    #LOGGER.info('minutes: '+str(minutes))
    #figure out our timestamp to query back to and run query send results out
    queryTime = datetime.datetime.utcnow()-datetime.timedelta(minutes=minutes)
    query = '{"timestamp" : {"$gte": "'+str(queryTime)+'"}},{"score": 1, "latitude": 1, "longitude": 1})'
    try:
        mongoResults = collection.find({"timestamp" : {"$gte": queryTime}}, {"score": 1, "latitude": 1, "longitude": 1})
    except Exception as e:
        LOGGER.error('Could not execute query against the scores collection: {}'.format(str(e)))
        sys.exit(1)

    LOGGER.info('Found {} results from query {}'.format(mongoResults.count(with_limit_and_skip=False), query))
    geoScores = getGeobject(json_util.dumps(mongoResults))
    return respond(None, geojson.dumps(geoScores))

''' Gets the top scores from MongoDB from the past hour'''
def getTopScores(event, context):
    mongo_client = mdbClient()
    try:
       # check to make sure we can connect to database.
        mongo_client.admin.command('ismaster')
    except ConnectionFailure:
        raise Exception("MongoDB connection failed")

    db = mongo_client[DATABASE_NAME]
    collection = db['Scores']
    minutes = 60

    #figure out our timestamp to query back to and run query send results out
    queryTime = datetime.datetime.utcnow()-datetime.timedelta(minutes=minutes)
    query = '{"timestamp" : {"$gte": "'+str(queryTime)+'"}}, {"score": 1, "firstname": 1}).sort({"score": -1}).limit(25)'
    try:
        mongoResults = collection.find({"timestamp" : {"$gte": queryTime}}, {"score": 1, "firstname": 1}).sort([("score", -1)]).limit(25)
    except Exception as e:
        LOGGER.error('Could not execute query against the scores collection: {}'.format(str(e)))
        sys.exit(1)

    LOGGER.info('Found {} results from query {}'.format(mongoResults.count(with_limit_and_skip=False), query))
    return respond(None, json_util.dumps(mongoResults))
