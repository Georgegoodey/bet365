import time
from csv import reader
from pymongo import MongoClient
from multiprocessing import Pool

def cmt():
    return round(time.time() * 1000)

def getData():
    dataStart = cmt()
    header = ['item', 'date']
    csvFile = open('customer_orders.csv', 'r')

    dbData = []
    customers = []

    with open('customer_orders.csv','r') as csvfile:
        data = reader(csvfile, delimiter = ',')
        *_, last = data

    with open('customer_orders.csv','r') as csvfile:
        data = reader(csvfile, delimiter = ',')
        csvRow = next(data)
        name = csvRow[0]
        customer = {"name": name, "orders": []}
        while(csvRow != last):
            row = {}
            for i in header:
                row[i] = csvRow[header.index(i)+1]
            if(name != csvRow[0]):
                dbData.append(customer)
                name = csvRow[0]
                customer = {"name": name, "orders": []}
            else:
                customer["orders"].append(row)
            csvRow = next(data)
        row = {}
        for i in header:
            row[i] = csvRow[header.index(i)+1]
        customer["orders"].append(row)
        dbData.append(customer)
        dataEnd = cmt()
        dataSortTime = dataEnd - dataStart
        print(f"Created Database List "+str(dataSortTime)+" Millis")
        return dbData

def startDatabase():
    mongoClient = MongoClient("mongodb://localhost:27017")
    db = mongoClient.bet365
    if 'customers' in db.list_collection_names():
        collection = db['customers']
        collection.drop()
    collection = db.customers

def insertData(dbData):
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