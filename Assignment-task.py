import json
import flatten_json
import pymongo
def  assignment_task():
  
      myclient = pymongo.MongoClient("mongodb+srv://pymong:Mx40Yof7286Zr91H@db-mongodb-f6a21137.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb")
      mydb = myclient["Local_PickingSupervisor"]
      
      mycol = mydb["Assignment"]
      myquery =mycol.find({},{'_id':0})   
      for document in myquery:
          doc={key: document[key] for key in document.keys() & {'AssignmentId'}}
          print(document)     
          print(doc)   
          mydb.Task.update_many(doc, {"$set": document},upsert=True) 
          print("Test")             
assignment_task()
