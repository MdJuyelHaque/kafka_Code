
from time import sleep
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
import flatten_json
#channel
topic='Task1-DB-Task'

# producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'),)

#consumer
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],value_deserializer=lambda x:json.loads(x.decode('utf-8')),auto_offset_reset='latest', enable_auto_commit=True,group_id="test")

#consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],auto_offset_reset='latest', enable_auto_commit=True)
message=None
table_name=None
payload=None
list1=[]
for message in consumer:
    message = message.value
    print(message)
    payload=message['payload']
    print(payload)
    flat_josn = flatten_json.flatten(message, ".")
    fl_json=json.dumps(flat_josn, indent = 2)
    json_load=json.loads(fl_json)
    table_name=json_load["schema.name"]
    print(table_name)
    myclient = pymongo.MongoClient("mongodb-connectionstring")
    
    mydb = myclient[table_name]
    mycol = mydb[table_name]
    x = mycol.insert_one(payload)
    # myquery =payload
    # print(myquery)
    # newvalues = { "$set":payload }
    # print(newvalues)
    # mycol.update_one(myquery,newvalues,upsert="true")

# p1={"BookName":"Juyel"}
# p={"$set":{"BookName":"Juyel"}}
    #ycol.update_one(myquery,newvalues)
    #mycol.update_one({'_id':x}, {"$set": payload}, upsert=True)
    # x1=mydb.mycol.find({},'BookId')
    # print(x1)
    
    

 

    
    
