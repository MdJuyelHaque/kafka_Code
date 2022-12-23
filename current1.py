from time import sleep
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
import flatten_json
#channel
# topic='Books-Books'
# topic1='UWP-UnitOfWork'
# topic='t1-Task'
topic1='UOW-UnitOfWork'
topic='Task_New-Task1'

# producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'),)
def  table1():

#consumer
  consumer = KafkaConsumer(topic,topic1,bootstrap_servers=['localhost:9092'],value_deserializer=lambda x:json.loads(x.decode('utf-8')),auto_offset_reset='latest', enable_auto_commit=True,auto_commit_interval_ms=1000,group_id="test")
  message=None
  table_name=None
  payload=None
  list1=[]
  for message in consumer:
      message = message.value
      payload=message['payload']
      print(payload)
      flat_josn = flatten_json.flatten(message, ".")
      fl_json=json.dumps(flat_josn, indent = 2)
      json_load=json.loads(fl_json)
      table_name=json_load["schema.name"]
      print(table_name)
      myclient = pymongo.MongoClient("mongodb+srv://pymong:Mx40Yof7286Zr91H@db-mongodb-f6a21137.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb")
      mydb = myclient["Local_PickingSupervisor"]
      mycol = mydb[table_name]
    #   x = mycol.insert_one(payload)
      #myquery =mycol.find_one()
      
      
      print("The original dictionary : " + str(payload))
      res = {key: payload[key] for key in payload.keys()
                    & {'TaskId','UnitOfWorkId'}}
      print("The filtered dictionary is : " + str(res))      

      mydb.UnitOfWork_Test.update_many(res, {"$set":  payload},upsert=True) 
      
      

        
      
      
      # res1 = {key: payload[key] for key in payload.keys()
      #           & {'BId'}}
      # print("The filtered dictionary is : " + str(res))      

      # mydb.Books.update_many(res1, {"$set":  payload },upsert=True) 
      # print(myquery)
      # newvalues = { "$set":payload }
      # print(newvalues)
      # mycol.update_one(myquery,newvalues)
      
table1()
