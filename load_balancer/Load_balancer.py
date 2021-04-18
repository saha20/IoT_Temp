#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 22:56:56 2021

@author: aman
"""
from flask import Flask,request, jsonify
import pymongo 
import requests
import sys
import socket

dburl = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
myclient = pymongo.MongoClient(dburl)
mydb = myclient[db_name]
appcol = mydb["application_log"]
hostcol = mydb["host_log"]
app = Flask(__name__)
app.config['DEBUG'] = True
@app.route("/return_host", methods=["GET", "POST"])
def returnServer():
 # get server with lowest request handling
 l = hostcol.find()     #get_hosts
 loads={}
 if l.count()==0:
     return jsonify(
     status=0
 )
 for host in l:
     URL="http://"+host["name"]+":5000/get_load"
     r = requests.get(url = URL).json()
     tup=(r["mem_load"],r["cpu_load"])
     loads[host["name"]]=tup
 #print(loads,file=sys.stderr)    
 srt=sorted([(value,key) for (key,value) in loads.items()])
 print("Deploy on ",srt[0],file=sys.stderr)
     
 return jsonify(
     machine_name=srt[0]
 )
@app.route("/log_service", methods=["GET", "POST"])
def logService():
 # log in db, which application is running in which machine/server
 # we require user_id, application_id, ip, port
 content=request.json
 #item to search
 item={"app_id":content["app_id"],"user_id":content["user_id"],"service_name_to_run":content["service_name_to_run"],"service_id":content["service_id"]}
 
 retr= appcol.find_one(item) #find item
 
 if not retr:   #item not found
     item["ip"]=content["ip"]
     appcol.insert_one(item)         #new entry
 else:          #item found
    return jsonify(
        status="0"
        )

 return jsonify(
 status="1"
 )


@app.route("/free_service", methods=["GET", "POST"])
def freeService():
 # if a machine is not handling any request close it.
 # we require user_id, application_id, ip, port
 content=request.json
 #item to search
 item={"app_id":content["app_id"],"user_id":content["user_id"],"service_name_to_run":content["service_name_to_run"],"service_id":content["service_id"],"ip":content["ip"]}
 
 retr= appcol.find_one(item) #find item
 
 if not retr:   #item not found
     return jsonify(
         status="0"
         )
 
 appcol.delete_one(item)
 return jsonify(
 status="1"
 )
if __name__ == "__main__":
 # change to app.run(host="0.0.0.0"), if you want other machines to be able to reach the webserver.
 app.run(host=socket.gethostbyname(socket.gethostname()),port=5000) 
