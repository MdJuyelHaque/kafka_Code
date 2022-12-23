import json
import flatten_json
import pymongo
def  unit_task():
  
      myclient = pymongo.MongoClient("mongodb")
      mydb = myclient["db"]
      mydb1=myclient["db"]
      
      mycol = mydb["Assignment"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
        doc={key: document[key] for key in document.keys() & {'AssignmentId'}}
        print(document)     
        print(doc)   
        mydb.UnitOfWork.update_many(doc, {"$set":  document},upsert=True) 
        print("Test")            
unit_task()
