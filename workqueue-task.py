import json
import flatten_json
import pymongo
def  workqueue_task():
  
      myclient = pymongo.MongoClient("mongodb-connectionstring")
      mydb = myclient["Dbname"]
      
      mycol = mydb["WorkQueue"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'WorkQueueId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
workqueue_task()
