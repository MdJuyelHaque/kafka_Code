import json
import flatten_json
import pymongo
def  task_state_task():
  
      myclient = pymongo.MongoClient("mongodb+srv://pymong:Mx40Yof7286Zr91H@db-mongodb-f6a21137.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb")
      mydb = myclient["Local_PickingSupervisor"]
      
      mycol = mydb["TaskState"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'LastPickedUserDisplayName'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True)             
task_state_task()
