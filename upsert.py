# Use Python 3
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import re
import json
import math


##add these as environement varibles
# ELASTIC_CLOUD_KEY = 'CC-project:ZXUtd2VzdC0xLmF3cy5mb3VuZC5pbyQ2MTVmMWVmNWZmM2M0YjUzODViMWY0NjVjZjE2NDkwOSQyOTMyZjQ2YWM3NzQ0ZDlhOGY5N2YyNmQ1ZTE3ZWNlOQ=='
# ELASTIC_API_USERNAME ='elastic'
# ELASTIC_API_PASSWORD ='PsZ1Dg1A2eo8fcIOeT2QkSr8'


ELASTIC_CLOUD_KEY = os.environ['ELASTIC_CLOUD_KEY']
ELASTIC_API_USERNAME = os.environ['ELASTIC_API_USERNAME']
ELASTIC_API_PASSWORD = os.environ['ELASTIC_API_PASSWORD']

ES_URL = 'localhost:9200'
ES_INDEX = 'test_upsert'
DATA_FILE = 'test.json'
PAGE_SIZE = 10000
USE_BULK = True

def upsert(es, doc, id):
    es.update(
        index=ES_INDEX,
        id=id,
        body={
            'doc':doc,
            'doc_as_upsert':True
    })
    return

def bulk_upsert(es, data):
    helpers.bulk(es, data, index=ES_INDEX)
    return

def Load_data_form_file():
   with open(DATA_FILE, 'r') as filehandle:
    filecontent = filehandle.read()
    return filecontent


def main():
    # read data form source
    data = Load_data_form_file()
    entries = re.split('(\{.*?\})(?= *\{)', data)
    entries = [entry for entry in entries if entry]

    # Init ES client
    es = Elasticsearch([ES_URL], verify_certs=True, timeout=30)

    # Test the conenction
    if not es.ping():
        raise ValueError("Connection to ES failed")

    # Index will automatically created if it does not exist, Auto mapping is supported too in ES 7.x Versions
    print('Connected to ES!')

    total_entries = len(entries)
    print('Inserting or updating ' + str(total_entries) + ' entries')

    updated_entries = 0
    
    # Used if bulk flag is used
    data = []
    
    for entry in entries:
        try:
            json_obj =  json.loads(entry)
            index = str(json_obj['index'])
        except ValueError:
            print('Unable to decode' + entry)
            continue
        
        if USE_BULK:
            data.append({
                "_op_type": 'update',
                "_index": ES_INDEX,
                "_id": index,
                "doc": json_obj,
                "doc_as_upsert":True
            })
            continue

        upsert(es, json_obj, index)

        updated_entries += 1
        percentage = 0
        if updated_entries / total_entries == 0.25:
            percentage = 25
        elif updated_entries / total_entries == 0.5:
            percentage = 50
        elif updated_entries / total_entries == 0.75:
            percentage = 75
        
        if percentage > 0:
            print(str(percentage) + '% of data inserted or updated!')

    if USE_BULK:
        pages = math.ceil(total_entries / PAGE_SIZE)
        page_counter = 1
        while page_counter <= pages:
            start = (page_counter - 1) * PAGE_SIZE
            end = start + PAGE_SIZE
            print(start)
            print(end)
            if end > total_entries:
                end = total_entries

            helpers.bulk(es, data[start:end], index=ES_INDEX)
            
            page_counter += 1
    
    print("Data updated Successfully!")
    return

if __name__ == "__main__":
    # execute only if run as a script
    main()