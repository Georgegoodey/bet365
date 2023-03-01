import time
import csv
from pymongo import MongoClient
from multiprocessing import Pool

def cmt():
    return round(time.time() * 1000)

def getData():
    dataStart = cmt()
    header = ['name', 'item', 'date']
    csvFile = open('customer_orders.csv', 'r')

    dbData = []

    with open('customer_orders.csv','r') as csvfile:
        data = csv.reader(csvfile, delimiter = ',')
        *_, last = data

    with open('customer_orders.csv','r') as csvfile:
        data = csv.reader(csvfile, delimiter = ',')
        csvRow = next(data)
        
        while(csvRow != last):
            row = {}
            for i in header:
                row[i] = csvRow[header.index(i)]
            dbData.append(row)
            csvRow = next(data)
        row = {}
        for i in header:
            row[i] = csvRow[header.index(i)]
        dbData.append(row)
        dataEnd = cmt()
        dataSortTime = dataEnd - dataStart
        print(f"Sorted database In "+str(dataSortTime)+" Millis")
        return dbData

def startDatabase():
    mongoClient = MongoClient("mongodb://localhost:27017")
    db = mongoClient.bet365
    if 'customers' in db.list_collection_names():
        collection = db['customers']
        collection.drop()
    collection = db.customers

def insertData(dbData):
    mongoClient = MongoClient("mongodb://localhost:27017")
    db = mongoClient.bet365
    collection = db.customers
    collection.insert_many(dbData)

if __name__ == '__main__':
    sTime = cmt()

    print("Starting Program")
    dTime = 0
    data = getData()
    pool = Pool(processes=8)
    chunkSize = len(data)//8
    chunks = [data[i:i+chunkSize] for i in range(0, len(data), chunkSize)]

    startDatabase()
    pool.map(insertData, chunks)

    pool.close()
    pool.join()
    eTime = cmt()
    timePassed = eTime - sTime
    print(f"Total Time Taken: "+str(timePassed)+" Millis")


    #mongodb+srv://Admin:guHURrbGhWRsfERb@cluster0.gs3a8t9.mongodb.net/test