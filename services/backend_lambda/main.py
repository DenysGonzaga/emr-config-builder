import os
import json
import boto3
from collections import namedtuple

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['DYN_TABLE_NAME']
table = dynamodb.Table(table_name)

ClusterData = namedtuple('ClusterData', ['master_instance', 
                                         'node_instance', 
                                         'node_count',
                                         'master_yarn_cores',
                                         'node_yarn_cores',
                                         'cores_executor',
                                         'reserved_cores'])

def default_response(body, statusCode=200):
     return {
               'statusCode': statusCode,
               'body': json.dumps(body),
               'headers': {'Content-Type': 'application/json'},
          }


def get_dbitem(key):
     k = {
          "category": "instance_data",
          "key": key
     }      
     return table.get_item(Key=k).get('Item')
     

def post_handler(data):
     return {}


def get_handler(data):
     master_instance = data.get('master_instance', None)
     node_instance =  data.get('node_instance', None)
     node_count = int(data.get('node_count', 1))
     master_yarn_cores = data.get('master_yarn_cores', None)
     node_yarn_cores = data.get('node_yarn_cores', None)
     cores_executor = data.get('node_yarn_cores',None)
     cores_executor = int(data.get('cores_executor', 2))
     reserved_cores = int(data.get('reserved_cores', 1))       
               
     ## Basic Data Validations    
     if master_instance is None or node_instance is None:
          return {"Response": "Master (master_instance) and Node (node_instance) query string parameters must be supplied."}  
     
     master_instance = get_dbitem(master_instance)
     node_instance = get_dbitem(node_instance)

     if master_instance is None:
          return {"Response": "Master (master_instance) not found."}
     
     if node_instance is None:
          return default_response({"Response": "(node_instance) not found."}, 400)

     if master_instance.get('yarn.cores') is None and \
          master_yarn_cores is None:
          return default_response({"Response": "Master yarn.cores at database is null, please change instance type or set" \
                                   " master_yarn_cores parameter on your request."}, 400)

     if node_instance.get('yarn.cores') is None and \
          node_yarn_cores is None:
          return default_response({"Response": "Instance yarn.cores at database is null, please change " \
                                   " instance type or set node_yarn_cores parameter on your request."}, 400) 

     return default_response({"master": master_instance, "nodes": node_instance})


def handler(event, context):
     try:
          httpMethod = event.get('httpMethod')

          if httpMethod == 'POST':
               response_data = post_handler(json.loads(event.get('body')))
          elif httpMethod == 'GET':
               response_data = get_handler(event.get('queryStringParameters'))
 
          return response_data     
     except Exception as e:
          return default_response({"Error": str(e)}, 500) 