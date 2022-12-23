import json
import flatten_json
import pymongo
def  BuildingPriorityCode_task():
  
      myclient = pymongo.MongoClient("mongodb")
      mydb = myclient["DB"]
      
      mycol = mydb["BuildingPriorityCode"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'BuildingName'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
BuildingPriorityCode_task()
