import os
import json
import boto3
import logging

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['DYN_TABLE_NAME']
table = dynamodb.Table(table_name)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(sh)


def default_response(body, statusCode=200):
    return {
        'statusCode': statusCode,
        'body': json.dumps(body),
        'headers': {'Content-Type': 'application/json'},
    }


def get_dbitem(key):
    try:
        k = {
            "category": "instance_data",
            "key": key
        }
        return table.get_item(Key=k).get('Item')
    except Exception as e:
        m = f"Problem on database reading. ERR: {str(e)}"
        logger.error(m)
        return default_response({"error": m}, 500)


def setOrUpdate_dbitem(item):
    try:
        return table.put_item(Item=item)
    except Exception as e:
        m = f"Problem on database upsert. ERR: {str(e)}"
        logger.error(m)
        return default_response({"error": m}, 500)


def post_handler(data):
    logger.info("Processing post request.")
    instance_type = data.get('instance.type')
    rvalue = {
        "mapreduce.map.java.opts" : data.get('mapreduce.map.java.opts'),
        "mapreduce.map.memory.mb" : data.get('mapreduce.map.memory.mb'),
        "mapreduce.reduce.java.opts" : data.get('mapreduce.reduce.java.opts'),
        "mapreduce.reduce.memory.mb" : data.get('mapreduce.reduce.memory.mb'),
        "yarn.app.mapreduce.am.resource.mb" : data.get('yarn.app.mapreduce.am.resource.mb'),
        "yarn.cores" : data.get('yarn.cores'),
        "yarn.nodemanager.resource.memory-mb" : data.get('yarn.nodemanager.resource.memory-mb'),
        "yarn.scheduler.maximum-allocation-mb" : data.get('yarn.scheduler.maximum-allocation-mb'),
        "yarn.scheduler.minimum-allocation-mb" : data.get('yarn.scheduler.minimum-allocation-mb')
    }

    null_keys = []
    if instance_type is None:
        null_keys.append("instance.type")

    for k, v in rvalue.items():
        if v is None:
            null_keys.append(str(k))

    if len(null_keys) > 0:
        str_nk = ', '.join(null_keys)
        w = "is" if len(null_keys) == 1 else "are"
        return default_response({"response": f"Post method must have all properties: '{str_nk}' {w} None."}, 400)
    
    try:
        int(rvalue.get("yarn.cores"))
    except:
        return default_response({"response": "Parameter 'yarn.cores' must be integer."}, 400)


    setOrUpdate_dbitem({
        "category": "instance_data",
        "key": instance_type,
        "value": rvalue
    })

    return default_response({"response": "Database item upserted."})


def get_handler(data):
    logger.info("Processing get request.")
    master_instance = data.get('master_instance', None)
    node_instance = data.get('node_instance', None)
    node_count = int(data.get('node_count', 1))
    master_yarn_cores = data.get('master_yarn_cores', None)
    node_yarn_cores = data.get('node_yarn_cores', None)
    cores_by_executor = int(data.get('cores_by_executor', 2))
    set_python3 = bool(int(data.get('set_python3', 0)))
    set_spark_glue_catalog = bool(int(data.get('set_spark_glue_catalog', 0)))
    set_hive_glue_catalog = bool(int(data.get('set_hive_glue_catalog', 0)))

    # Basic Data Validations
    logger.info("Performing basic validations.")
    if master_instance is None or node_instance is None:
        return {"response": "Master (master_instance) and Node (node_instance) query string parameters must be supplied."}

    master_instance = get_dbitem(master_instance)['value']
    node_instance = get_dbitem(node_instance)['value']

    if master_instance is None:
        return {"response": "Master (master_instance) not found."}

    if node_instance is None:
        return default_response({"response": "(node_instance) not found."}, 400)
    
    if master_instance.get('yarn.cores') is not None:
        master_yarn_cores = master_instance.get('yarn.cores')

    if master_instance.get('yarn.cores') is None and \
            master_yarn_cores is None:
        return default_response({"response": "Master yarn.cores at database is null, please change instance type or set "
                                "master_yarn_cores parameter on your request."}, 400)

    if node_instance.get('yarn.cores') is not None:
        node_yarn_cores = int(node_instance.get('yarn.cores'))

    if node_instance.get('yarn.cores') is None and \
            node_yarn_cores is None:
        return default_response({"response": "Node yarn.cores at database is null, please change "
                                "instance type or set node_yarn_cores parameter on your request."}, 400)

    # Consistency validations
    logger.info("Performing consistency validations.")
    if int(node_yarn_cores) < cores_by_executor:
        return default_response({"response": "cores_by_executor must be equal or lower than node_yarn_cores. Try either change parameter, instance type or node_yarn_cores value."}, 400)

    # Building configs
    logger.info("Building configurations.")
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

    return default_response({"response": emr_config})


def handler(event, context):
    try:
        logger.info("Request processing begun.")
        httpMethod = event.get('httpMethod')
        response_data = {}

        if httpMethod == 'POST':
           response_data = post_handler(json.loads(event.get('body')))
        elif httpMethod == 'GET':
           response_data = get_handler(event.get('queryStringParameters'))

        logger.info("Request processing ended. Preparing response")
        return response_data
    except Exception as e:
        logger.error(str(e))
        return default_response({"error": str(e)}, 500)
