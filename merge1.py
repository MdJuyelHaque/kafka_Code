from time import sleep
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
import flatten_json
from kafka import TopicPartition
#channel
# topic='Books-Books'
# topic1='UWP-UnitOfWork'
# topic='t1-Task'
topic1='Assignment-Assignment_Demo'
topic2='BuildingPriorityCode-BuildingPriorityCode_Demo'
topic3='TaskStage-TaskStagePickingType_Demo'
topic4='TaskState-TaskState_Demo'
topic5='UUnitOfWork_Demo'
topic6='UnitOfWorkLastPickedByUser-UnitOfWorkLastPickedByUser_Demo'
topic7='UnitOfWorkTaskAggregate_Demo-UnitOfWorkTaskAggregate_Demo'
topic8='User_Demo-User_Demo'
topic9='WorkQueue-WorkQueue_Demo'

topic=[topic1,topic2,topic3,topic4,topic5,topic6,topic7,topic8,topic9]

# producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'),)
def  table1():

#consumer
  consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],value_deserializer=lambda x:json.loads(x.decode('utf-8')),auto_offset_reset='latest', enable_auto_commit=True,auto_commit_interval_ms=100,group_id="test1",max_poll_records=10)
  message=None
  table_name=None
  payload=None
  consumer.subscribe(topics=topic)
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
      mydb = myclient["Local_PickingSupervisor_Demo"]
      mycol = mydb[table_name]
    
      print("The original dictionary : " + str(payload))
      res = {key: payload[key] for key in payload.keys()
                            & {'TaskId','UnitOfWorkId','TaskStagePickingTypeId','WorkQueueId','UnitOfWorkId','UnitOfWorkId','BuildingName','AssignmentId','DisplayName','LastPickedUserDisplayName'}}
      print("The filtered dictionary is : " + str(res))           
      mydb.Combine_JSON.update_many(res, {"$set":  payload},upsert=True) 
            
         
table1()
