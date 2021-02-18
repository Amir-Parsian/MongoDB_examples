import pandas as pd
import csv
import json
from bson.code import Code
from pymongo import MongoClient
db =  MongoClient().tweets_2020

map = Code("""function () {
        if (!this.hasOwnProperty('retweeted_status')){
            emit('unique', 1);
        }else{
            emit('retweet', 1);
        }
        
   
}""")

reduce = Code("""function (key, values) {
var total = 0;
for (var i = 0; i < values.length; i++) {
total += values[i];
}
return total;
}""")

result = db.m12_d31.map_reduce(map, reduce, "myresults")
for doc in result.find():
    print(doc)
