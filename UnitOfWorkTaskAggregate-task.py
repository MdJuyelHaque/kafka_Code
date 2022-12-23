import json
import flatten_json
import pymongo
def  UnitOfWorkTaskAggregate_task():
  
      myclient = pymongo.MongoClient("mongodb+-connectionstring")
      mydb = myclient["DBname"]
      
      mycol = mydb["UnitOfWorkTaskAggregate"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'UnitOfWorkId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
UnitOfWorkTaskAggregate_task()
