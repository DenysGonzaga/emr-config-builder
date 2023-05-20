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
    return data


def get_handler(data):
    master_instance = data.get('master_instance', None)
    node_instance = data.get('node_instance', None)
    node_count = int(data.get('node_count', 1))
    master_yarn_cores = data.get('master_yarn_cores', None)
    node_yarn_cores = data.get('node_yarn_cores', None)
    cores_by_executor = int(data.get('cores_by_executor', 2))
    reserved_cores = int(data.get('reserved_cores', 1))
    set_python3 = bool(int(data.get('set_python3', 0)))
    set_spark_glue_catalog = bool(int(data.get('set_spark_glue_catalog', 0)))
    set_hive_glue_catalog = bool(int(data.get('set_hive_glue_catalog', 0)))

    # Basic Data Validations
    if master_instance is None or node_instance is None:
        return {"Response": "Master (master_instance) and Node (node_instance) query string parameters must be supplied."}

    master_instance = get_dbitem(master_instance)['value']
    node_instance = get_dbitem(node_instance)['value']

    if master_instance is None:
        return {"Response": "Master (master_instance) not found."}

    if node_instance is None:
        return default_response({"Response": "(node_instance) not found."}, 400)
    
    if master_instance.get('yarn.cores') is not None:
        master_yarn_cores = master_instance.get('yarn.cores')

    if master_instance.get('yarn.cores') is None and \
            master_yarn_cores is None:
        return default_response({"Response": "Master yarn.cores at database is null, please change instance type or set "
                                "master_yarn_cores parameter on your request."}, 400)

    if node_instance.get('yarn.cores') is not None:
        node_yarn_cores = int(node_instance.get('yarn.cores'))

    if node_instance.get('yarn.cores') is None and \
            node_yarn_cores is None:
        return default_response({"Response": "Node yarn.cores at database is null, please change "
                                "instance type or set node_yarn_cores parameter on your request."}, 400)

    # Consistency validations
    if int(node_yarn_cores) < cores_by_executor:
        return default_response({"Response": "cores_by_executor must be equal or lower than node_yarn_cores. Try either change parameter, instance type or node_yarn_cores value."}, 400)

    # Building configs

    executor_per_instance = int(
        int(node_yarn_cores) / cores_by_executor)
    executor_memory = int(
        node_instance['yarn.nodemanager.resource.memory-mb']) / executor_per_instance

    spark_executors_memory = int(executor_memory * 0.90)
    spark_executor_memoryOverhead = int(executor_memory * 0.10)

    spark_driver_memory = int(
        int(master_instance['yarn.nodemanager.resource.memory-mb']) * 0.4)
    spark_driver_cores = master_yarn_cores
    spark_driver_memoryOverhead = int(
        int(master_instance['yarn.nodemanager.resource.memory-mb']) * 0.10)

    spark_executor_instances = (
        executor_per_instance * node_count) - 1
    spark_default_parallelism = spark_executor_instances * cores_by_executor * 2

    emr_config = [{
        "Classification": "yarn-site",
        "Properties": {
            "yarn.app.mapreduce.am.resource.mb": node_instance["yarn.app.mapreduce.am.resource.mb"],
            "yarn.scheduler.minimum-allocation-mb": node_instance["yarn.scheduler.minimum-allocation-mb"],
            "yarn.scheduler.maximum-allocation-mb": node_instance["yarn.scheduler.maximum-allocation-mb"],
            "yarn.nodemanager.resource.memory-mb": node_instance["yarn.nodemanager.resource.memory-mb"]
        }
    },
    {
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "false"
        }
    },
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.dynamicAllocation.enabled": "false",
            "spark.driver.memory": "{}M".format(spark_driver_memory),
            "spark.executor.memory": "{}M".format(spark_executors_memory),
            "spark.executor.cores": str(spark_driver_cores),
            "spark.executor.instances": str(spark_executor_instances + 1),
            "spark.executor.memoryOverhead": "{}M".format(spark_executor_memoryOverhead),
            "spark.driver.memoryOverhead": "{}M".format(spark_driver_memoryOverhead),
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
            "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
            "spark.default.parallelism": str(spark_default_parallelism),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }
    },
    {
            "Classification": "mapred-site",
            "Properties": {
                "mapreduce.map.output.compress": "true",
                "mapreduce.map.java.opts": node_instance["mapreduce.map.java.opts"],
                "mapreduce.reduce.java.opts": node_instance["mapreduce.reduce.java.opts"],
                "mapreduce.map.memory.mb": node_instance["mapreduce.map.memory.mb"],
                "mapreduce.reduce.memory.mb": node_instance["mapreduce.reduce.memory.mb"]
            }
    }]

    if set_python3:
        emr_config.append({
            "Classification": "spark-env",
            "Configurations": [{
            "Classification": "export",
            "Properties": {
                "PYSPARK_PYTHON": "/usr/bin/python3"
            }
            }],
            "Properties": {}
        })

    if set_spark_glue_catalog:
        emr_config.append({
            "Classification":"spark-hive-site",
            "Properties":{
                "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
            "Configurations":[

            ]
        })
    
    if set_hive_glue_catalog:
        emr_config.append({
            "Classification":"hive-site",
            "Properties":{
                "hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
            "Configurations":[

            ]
        })

    return default_response({"emr_config": emr_config})


def handler(event, context):
    try:
        httpMethod = event.get('httpMethod')
        response_data = {}

        if httpMethod == 'POST':
            response_data = post_handler(json.loads(event.get('body')))
        elif httpMethod == 'GET':
            response_data = get_handler(event.get('queryStringParameters'))

        return response_data
    except Exception as e:
        return default_response({"Error": str(e)}, 500)

