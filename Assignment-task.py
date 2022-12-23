import json
import flatten_json
import pymongo
def  assignment_task():
  
      myclient = pymongo.MongoClient("mongodb")
      mydb = myclient["db"]
      
      mycol = mydb["Assignment"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'AssignmentId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True) 
          print("Test")             
assignment_task()
