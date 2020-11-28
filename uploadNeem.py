
import os
import sys
from pymongo import MongoClient
from datetime import datetime
import subprocess

MONGO_HOST = os.environ.get('WRITE_NEEM_MONGO_HOST')
MONGO_PORT = int(os.environ.get('WRITE_NEEM_MONGO_PORT'))
MONGO_DB = os.environ.get('WRITE_NEEM_MONGO_DB')
MONGO_USER = os.environ.get('WRITE_NEEM_MONGO_USER')
MONGO_PASS = os.environ.get('WRITE_NEEM_MONGO_PASS')
FROM_PREFIX = '5fbd2f586fc62ec298f26c1c'

# Get the path to the neem
if (len(sys.argv) == 1):
	PATH_TO_NEEM = sys.argv[0]
	print PATH_TO_NEEM
else:
	# Alternativly fix path here
	PATH_TO_NEEM = '/home/sascha/Desktop/1606230498.3804111'

# remote mongo client connection
connection = MongoClient(MONGO_HOST, MONGO_PORT)
mongoDbClient = connection['neems']
mongoDbClient.authenticate(MONGO_USER, MONGO_PASS, source='admin')

# Fill the meta data
metaCol = mongoDbClient['meta']

currentTime = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00");

mydict = {
    "created_by" : "Refills",
    "created_at" : currentTime,
    "model_version" : "0.1",
    "description" : "NEEM for scanning shelves.",
    "keywords" : [ 
        "Shelves", 
        "Robot",
        "Refills",
        "Retail"
    ],
    "url" : "neems/refills-2020-scanning shelves",
    "name" : "NEEM for robot scanning shelves",
    "activity" : {
        "name" : "Shelves scanning",
        "url" : "http://www.ease-crc.org/ont/SOMA.owl"
    },
    "environment" : "Retail",
    "image" : "https://www.innovations-report.de/wp-content/uploads/post-pictures/ro_686792-750x422.jpg",
    "agent" : "Robot"}

insertion_obj = metaCol.insert_one(mydict)
print(insertion_obj.inserted_id)

# remember the new prefix
TO_PREFIX = str(insertion_obj.inserted_id)
print(TO_PREFIX)

# Upload neem
cmd_prefix = "mongorestore --host=data.open-ease.org --port=28015 --username=mongoDBAdmin --password=iVCm6McaRMEFcNZ5qhVdSRPrf2DxoH --authenticationDatabase=admin -d neems -c "

triples_cmd = cmd_prefix + TO_PREFIX + "_triples " + PATH_TO_NEEM + "/triples/roslog/triples.bson"
inferred_cmd = cmd_prefix + TO_PREFIX + "_inferred " + PATH_TO_NEEM + "/inferred/roslog/inferred.bson"
annotations_cmd = cmd_prefix + TO_PREFIX + "_annotations " + PATH_TO_NEEM + "/annotations/roslog/annotations.bson"
tf_cmd = cmd_prefix + TO_PREFIX + "_tf " + PATH_TO_NEEM + "/ros_tf/roslog/tf.bson"

print subprocess.check_output(triples_cmd,stderr=subprocess.STDOUT,shell=True)
print subprocess.check_output(inferred_cmd,stderr=subprocess.STDOUT,shell=True)
print subprocess.check_output(annotations_cmd,stderr=subprocess.STDOUT,shell=True)
print subprocess.check_output(tf_cmd,stderr=subprocess.STDOUT,shell=True)

# copy indices
for name, index_info in mongoDbClient[FROM_PREFIX + '_triples'].index_information().iteritems():
	print name
	mongoDbClient[TO_PREFIX + '_triples'].create_index(keys=index_info['key'], name=name)

for name, index_info in mongoDbClient[FROM_PREFIX + '_inferred'].index_information().iteritems():
	print name
	mongoDbClient[TO_PREFIX + '_inferred'].create_index(keys=index_info['key'], name=name)

for name, index_info in mongoDbClient[FROM_PREFIX + '_annotations'].index_information().iteritems():
	print name
	mongoDbClient[TO_PREFIX + '_annotations'].create_index(keys=index_info['key'], name=name)

for name, index_info in mongoDbClient[FROM_PREFIX + '_tf'].index_information().iteritems():
	print name
	mongoDbClient[TO_PREFIX + '_tf'].create_index(keys=index_info['key'], name=name)
