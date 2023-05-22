import os
import json
import boto3
import requests
import logging
import functools as fn
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(sh)

yarn_data_url = os.environ['YARN_DATA_URL']
cores_data_url = os.environ['CORES_DATA_URL']
table_name = os.environ['DYN_TABLE_NAME']


dynamodb = boto3.resource('dynamodb')


def default_response(body, statusCode=200):
    return {
        'statusCode': statusCode,
        'body': json.dumps(body),
        'headers': {'Content-Type': 'application/json'},
    }


def handler(event, context):
    try:
        logger.info(
            "Crawling YARN data from AWS source ({})".format(yarn_data_url))

        page = requests.get(yarn_data_url, verify=False)
        soup = BeautifulSoup(page.text, 'html.parser')

        divs = soup.find_all('div', class_='table-container')
        tables = [t.find_all('table')[0] for t in divs]
        instances = {}

        for table in tables:
            instance = table.find_all(class_="table-header")[0].text.strip()
            instance_data = {}

            if len(table.find_all('th')) == 4:
                for row in range(0, len(table.find_all('td')), 3):
                    instance_data[table.find_all('td')[row].text] = table.find_all('td')[
                        #TODO: hbase handler 
                        #row + (2 if cluster_with_hbase else 1)
                        row + 2].text
            else:
                for row in range(0, len(table.find_all('td')), 2):
                    instance_data[table.find_all('td')[row].text] = table.find_all('td')[
                        row + 1].text

            instance_data["yarn.cores"] = None
            instances[instance] = instance_data

        logger.info(
            "Crawling CPU CORES data from AWS source ({})".format(cores_data_url))

        c_page = requests.get(cores_data_url, verify=False)
        c_soup = BeautifulSoup(c_page.text, 'html.parser')
        c_divs = c_soup.find_all('div', class_='lb-content-wrapper')
        c_list_tables = [t.find_all('table') for t in c_divs]

        for c_tables in c_list_tables:
            for c_table in c_tables:
                c_rows = c_table.find_all('tr')
                for row in c_rows:
                    splited_line = row.text.split("\n")
                    inst_type = splited_line[1]
                    inst_vcpu = splited_line[2]

                    if inst_type in instances:
                        instances[inst_type]["yarn.cores"] = int(inst_vcpu)
   
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for k, v in instances.items():
                batch.put_item(Item={"category": "instance_data",
                                     "key": k, "value": v})

        logger.info("Database updated.")
        default_response({"response": "Database updated."})
    except Exception as e:
        logger.error(str(e))
        default_response({"error": str(e)}, statusCode=404)
