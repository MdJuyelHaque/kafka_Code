import json
import flatten_json
import pymongo
def  taskstate_task():
  
      myclient = pymongo.MongoClient("mongodb")
      mydb = myclient["DB"]
      
      mycol = mydb["TaskState"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'TaskId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
taskstate_task()
