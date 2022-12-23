import json
import flatten_json
import pymongo
def  task_state_task():
  
      myclient = pymongo.MongoClient("mongodb")
      mydb = myclient["DB"]
      
      mycol = mydb["TaskState"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'LastPickedUserDisplayName'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
task_state_task()
