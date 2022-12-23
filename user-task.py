import json
import flatten_json
import pymongo
def  User_task():
  
      myclient = pymongo.MongoClient("mongodb-connectionstring")
      mydb = myclient["DBName"]
      
      mycol = mydb["User"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'DisplayName'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
User_task()
