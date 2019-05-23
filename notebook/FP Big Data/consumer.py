from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import os
import io

consumer = KafkaConsumer(
    'BigData041',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.getcwd(), 'newdataset_kafka')
batch_limit = 400000
batch_counter = 0
batch_number = 0
try:
    while True:
		for message in consumer:
		    if batch_counter >= batch_limit:
		        batch_counter = 0
		        batch_number += 1
		        writefile.close()
		    if batch_counter == 0:
		        file_path = os.path.join(folder_path, ('model' + str(batch_number) + '.csv'))
		        writefile = io.open(file_path, "w")
		    message = message.value
		    # data = '{}'.format(message) 		    
		    writefile.write(message)
		    batch_counter += 1
		    print('current batch : ' + str(batch_number) + ' current data for this batch : ' + str(batch_counter))
		    print(message)
       

except KeyboardInterrupt:
    writefile.close()
    print('Keyboard Interrupt called by user, exiting.....')

