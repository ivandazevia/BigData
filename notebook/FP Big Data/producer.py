from time import sleep
from json import dumps
from kafka import KafkaProducer
import csv
import os
import io
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
counter = -1
dataset_folder_path = os.path.join(os.getcwd(), 'newdataset')
dataset_file_path = os.path.join(dataset_folder_path, 'BookCrossing.csv')
with io.open(dataset_file_path,"rt") as f:
    for row in f:
        counter += 1
        if counter == 0:
            continue
        # writefile.write(row)
        producer.send('BigData041', value=row)
        print(row.encode('utf-8'))
        sleep(0.001)


