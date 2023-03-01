import csv 
import pymongo
import datetime

mongoClient = pymongo.MongoClient("mongodb://192.168.56.1:27017/")
print(mongoClient.list_database_names())
header = ['name', 'item', 'date']
db = mongoClient['bet365']
collection = db['customers']

csvFile = open('customer_orders.csv', 'r')

startTime = datetime.datetime.now()

with open('customer_orders.csv','r') as csvfile:
    data = csv.reader(csvfile, delimiter = ',')
    *_, last = data

with open('customer_orders.csv','r') as csvfile:
    # print(last)
    data = csv.reader(csvfile, delimiter = ',')
    csvRow = next(data)
    for n in range(10):
    # while(csvRow != last):
        row = {}
        for i in header:
            row[i] = csvRow[header.index(i)]
        collection.insert_one(row)
        csvRow = next(data)
    row = {}
    for i in header:
        row[i] = csvRow[header.index(i)]
    # collection.insert_one(row)

# collection.findOne()

endTime = datetime.datetime.now()
timePassed = endTime - startTime
print(f"Time taken: "+str(timePassed.microseconds))
    #mongodb+srv://Admin:guHURrbGhWRsfERb@cluster0.gs3a8t9.mongodb.net/test