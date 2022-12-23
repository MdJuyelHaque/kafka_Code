import json
import flatten_json
import pymongo
def  unit_task():
  
      myclient = pymongo.MongoClient("mongodb-connectionstring")
      mydb = myclient["DB"]
      
      mycol = mydb["UnitOfWork"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'UnitOfWorkId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
unit_task()
