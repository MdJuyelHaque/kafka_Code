
from time import sleep
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
import flatten_json
#channel
topic='multi-update-Books'

# producer
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'),)
def  table1():

# #consumer
#   consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],value_deserializer=lambda x:json.loads(x.decode('utf-8')),auto_offset_reset='latest', enable_auto_commit=True,auto_commit_interval_ms=1000,group_id="test")
#   message=None
#   table_name=None
#   payload=None
#   list1=[]
  # for message in consumer:
  #     message = message.value
  #     payload=message['payload']
  #     print(payload)
  #     flat_josn = flatten_json.flatten(message, ".")
  #     fl_json=json.dumps(flat_josn, indent = 2)
  #     json_load=json.loads(fl_json)
  #     table_name=json_load["schema.name"]
  #     print(table_name)
  payload={'BookId': 11793, 'BookName': 'Java1211', 'CreateDate': 1669100090430, 'ModifiedDate': 1669100090430}
  myclient = pymongo.MongoClient("mongodb+srv://lucas-system:xk1795Bqjv2I6m80@db-mongodb-f6a21137.mongo.ondigitalocean.com/admin?tls=true&authSource=admin")
  
  mydb = myclient["Books"]
  mycol = mydb["Books"]
  list=[]

  print("The original dictionary : " + str(payload))
  res = {key: payload[key] for key in payload.keys()
            & {'BookId'}}
  print("The filtered dictionary is : " + str(res))      

  mydb.Books.update_many(res, {"$set":  payload }) 
      
table1()



 

    
 

    
    
