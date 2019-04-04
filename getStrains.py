# dump_strains.py
# Author: Rohit Kaundal
# Date: 24 Feb 2019 11:23PM IST
#Description: This script connects to waven cloud storage and dumps strains data in chunks of 500 records

import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import json
import os
import csv
import pyfiglet
import time
import sys
from google.cloud import storage
from shutil import copyfile
import re
import pandas
import urllib3
import pathlib

creds = credentials.Certificate('./firestore-key.json') # Load API credentials key
fbApp = firebase_admin.initialize_app(creds) # Initialise firebase app
strainsNotAdded = []

firestoreDb = firestore.client(fbApp) # Init firestore db

strainsRef = firestoreDb.collection(u'strains2') # get reference to strains db
strainsBackup = firestoreDb.collection(u'strains_backup') # backup strains from strains2 collection
thumbnailPath = "https://storage.googleapis.com/waven-backend.appspot.com/prods-img-thumb/" # ref to file directory of our thumbnails

def getBackupCount():
    docs = strainsBackup.get()
    docCount = 0
    for doc in docs:
        docCount += 1
    print("[+] Total Backup Strains: {}".format(docCount))

def getStrainCount():
    docs = strainsRef.get()
    docCount = 0
    for doc in docs:
        docCount += 1
    print("[+] Total Strains in strains2 collection: {}".format(docCount))

def backupStrains():
    docs = strainsRef.get()
    for doc in docs:
        strainsBackup.add(doc.to_dict())
        print("Document backedup => {}".format(doc.to_dict().get('Name')))

def backupCollection(fromCollectionRef, toCollectionRef):
    docs = fromCollectionRef.get()
    errorDocs = []
    countSuccess = 0
    countFailure = 0
    counterPause = 0
    for doc in docs:
        counterPause += 1
        if counterPause % 500 == 0:
            print("[!] Pausing...., 500 requests reached")
            time.sleep(6)

        try:
            toCollectionRef.add(doc.to_dict())
            print("[+] Doc backed => {}".format(doc.to_dict()))
            countSuccess += 1
        except Exception as e:
            print("[-] Error copying doc: {}".format(doc))
            errorDocs.append(doc.to_dict)
            countFailure += 1
            continue
    if(len(errorDocs) > 0):
        print("[-] Error copying these docs: {}".format(errorDocs))
    else:
        print("[!] Backup {} records successfully updated with {} failed records !".format(countSuccess, countFailure))

def addStrainToStrains2(docObj):
    try:
        strainsRef.add(docObj)
        print("[+] Strain Added: {}".format(docObj.get('Name')))
    except Exception as e:
        print(e)
        strainsNotAdded.append(docObj)

def getFirstRecord(dbref):
    docs = strainsRef.limit(1).get()
    for doc in docs:
        print(doc.to_dict().keys())


def readStrainsFromFile(fileName):
    strainsJson = {}
    with open(fileName, encoding='UTF-8-sig') as f:
        # data = f.read()
        # strainsJson = json.loads(data)
        strainsJson = json.load(f)
    return strainsJson

def deleteHybridStrain():
    docs = strainsRef.where(u'Type', u'==', u'Hybrid').get()
    countDoc = 0
    for doc in docs:

        strainsRef.document(doc.id).delete()
        countDoc += 1

    print("[+] Hybrid Strains Deleted: {}".format(countDoc))

def deleteSativaStrain():
    docs = strainsRef.where(u'Type', u'==', u'Sativa').get()
    countDoc = 0
    for doc in docs:
        strainsRef.document(doc.id).delete()
        countDoc += 1

    print("[+] Sativa Strains Deleted: {}".format(countDoc))

def deleteIndicaStrain():
    docs = strainsRef.where(u'Type', u'==', u'Indica').get()
    countDoc = 0
    for doc in docs:
        strainsRef.document(doc.id).delete()
        countDoc += 1

    print("[+] Indica Strains Deleted: {}".format(countDoc))

def addStrainsFromFile(filename):
    print("[+] Adding Strains from File: {}".format(filename))
    data = readStrainsFromFile(filename)
    processData = data['Strain']  # List of dictionary of strains

    for doc in processData:
        dbRecord = {
            u'Name': doc.get('Name'),
            u'TotalReviews': doc.get('TotalReviews'),
            u'Effects': doc.get('Effects'),
            u'Medical': doc.get('Medical'),
            u'Rating': doc.get('Rating'),
            u'Type': doc.get('Type'),
            u'ProductDescription': doc.get('ProductDescription'),
            u'Flavours': doc.get('Flavours'),
            u'Negatives': doc.get('Negatives')
        }
        addStrainToStrains2(dbRecord)

def addAuthorNameToCollection(colRef):
    docs = colRef.get()
    try:
        for doc in docs:
            try:
                if(doc.to_dict().get('UpdatedBy', "") != u'Waven'):
                    colRef.document(doc.id).update({
                        u'UpdatedBy': U'Waven'
                    })
                    print("[+]Doc updated => {}".format(doc.to_dict()))
                    sys.stdout.flush()
            except Exception as e:
                print("[-] Error in loop... => {}".format(e))
                continue

    except Exception as e:
        print("[-] Error occured => {}".format(e))

def getProductsData():
    prodsRef = firestoreDb.collection('latestprods').limit(2)
    documents = prodsRef.get()
    return documents


def searchInList(searchKey, searchValue, searchList):
    result =  (doc for doc in searchList if doc[searchKey] == searchValue)
    return result

def loadDataFromFile(filename):
    try:
        with open(filename, encoding='UTF-8-sig') as f:
            jsonData = json.load(f)
            return jsonData
    except Exception as e:
        print("[+] Error occured => {}".format(e))

def xtractImgFromObj(objProduct):
    imgData = objProduct.get('FullImage', None)
    if( imgData):
        tmpSlice = imgData.split('/')
        print("Data extracted: {}".format(tmpSlice))
        return tmpSlice

def uploadFileToFirebase(file_name, file_path):
    storage_client = storage.Client.from_service_account_json('firestore-key.json')

    # buckets = list(storage_client.list_buckets())
    bucket = storage_client.get_bucket("waven-backend.appspot.com")
    stats = storage.Blob(bucket=bucket, name="prods-img-thumb/{}".format(file_name)).exists(storage_client)
    if stats:
        print("[-] Filename {} already exists, skipping upload !".format(file_name))
        os.remove(file_path)
    else:
        blob = bucket.blob('prods-img-thumb/{}'.format(file_name))
        blob.upload_from_filename(file_path)
        print("[+] File uploaded at: {}".format(blob.public_url))
        # sys.stdout.flush()

    return True

def uploadImgThumbsFromFolder(folderName):
    errFiles = []
    fileCount = 0;
    for root, dirs, files in os.walk(folderName):

        for filename in files:
            if fileCount % 500 == 0:
                print("[!] Sleeping for 2 seconds")
                sys.stdout.flush()
                time.sleep(2)

            filepath = os.path.join(root, filename)
            # print("[+] Processing: {}".format(filepath))
            # sys.stdout.flush()
            # print("[+] Filepath: {}".format(filepath))
            try:
                # dest = "./tmpimgs/{}".format(filename)
                uploadFileToFirebase(filename, filepath)
                # copyfile(filepath, dest)
                fileCount += 1

            except Exception as e:
                errFiles.append(filepath)
                print("[!] Error occured with {} => {}".format(filename, e))
                sys.stdout.flush()

    print("[+]Total files: {}".format(fileCount))
    for errFile in errFiles:
        print("[-] File {} not added !".format(errFile))
        sys.stdout.flush()


def updateImageOfProduct(prodName, imgUrl):
    tmpProdName = prodName
    tmpUrl = imgUrl

    #get product reference where ProductName == prodName
    prodRef = firestoreDb.collection(u'latestprods').where(u'ProductName', u'==', tmpProdName)
    docs =  list(prodRef.get())

    if docs:
        # print("[+] {} exists !".format(prodName))
        for doc in docs:
            jsonDoc = doc.to_dict()
            docId = doc.id
            tmpProdRef =  firestoreDb.collection(u'latestprods').document(docId).update({
                u'imageUrl': tmpUrl
            })
            print("[+] Updated Document ID: {} => {} with URL {}".format(docId, jsonDoc.get("ProductName", ""), imgUrl))

    else:
        print("[-] Product with {} name not found".format(prodName))


def loadImagesFromFile(fileName):
    tmpFname = fileName
    jsonProd = {}
    with open(tmpFname, encoding="UTF-8-sig") as f:
        jsonProd = json.load(f)
    # print("[+]Data {}".format(jsonProd))
    prodData = jsonProd.get('Product', None )
    if prodData:
        for prods in prodData:
            # print("[+] Product => {}, Image: {} ".format(prods.get("ProductName", "---Emtpy---"), prods.get("Img_url", "---EMPTY---")))
            prodName = prods.get("ProductName", "---Emtpy---")
            # imageUrl =  prods.get("Img_url", "---EMPTY---")
            imageUrl =  prods.get("FullImage", "---EMPTY---")

            if imageUrl != "---EMPTY---":
                imgUrl = imageUrl.split("/")
                fullImage = "https://storage.googleapis.com/waven-backend.appspot.com/prods-img-thumb/{}".format(imgUrl[2])
                # updateImageOfProduct(prodName, imageUrl)
                print("[+] Image url: {}".format(fullImage))
            else:
                print("[-] Image url not found !")
    else:
        print("[-]No products found !")

def processImageJsonFiles(folderName):
    for root, dirs, files in os.walk(folderName):
        for file in files:
            # print("[+] File: {}".format(os.path.join(root, file)))
            try:
                filePath = os.path.join(root, file)
                loadImagesFromFile(filePath)
            except Exception as e:
                print("[!] Error occured: {}".format(e))

def delProductsWithoutImages():
    prodRef = firestoreDb.collection(u'latestprods')
    docs = list(prodRef.get())
    if docs:
        for doc in docs:
            jsonDoc = doc.to_dict()
            docId = doc.id
            if (jsonDoc.get("imageUrl", None) == None):
                prodName = jsonDoc.get("ProductName")

                try:
                    prodRef.document(docId).delete()
                    print(f"[+]{docId} => {prodName} deleted!")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"[!] Error: {e}")


    else:
        print("[-] No product found")


def getProductsWithoutImages():
    prodRef = firestoreDb.collection(u'latestprods')
    docs = list(prodRef.get())
    if docs:
        for doc in docs:
            jsonDoc = doc.to_dict()
            docId = doc.id
            if(jsonDoc.get("imageUrl", None) == None):
                prodName = jsonDoc.get("ProductName")
                print(f"{docId},{prodName}")

    else:
        print("[-] No product found")

def countRecordsWithImages():
    # get product reference where ProductName == prodName
    prodRef = firestoreDb.collection(u'latestprods')
    docs = list(prodRef.get())
    countRecordWithImages = 0;
    if docs:
        # print("[+] {} exists !".format(prodName))
        for doc in docs:
            jsonDoc = doc.to_dict()
            docId = doc.id
            if(jsonDoc.get("imageUrl", None)):
                countRecordWithImages += 1

        print(f"[=] Total record with images: {countRecordWithImages}")
    else:
        print("[-] Error connecting database")


def loadProdsFromXLSX(filename):
    tmpFilename = filename
    concentratesSolvent = pandas.read_excel(f'json_products/{tmpFilename}')
    df = pandas.DataFrame(concentratesSolvent, columns=['prod-name', 'prod-image-src'])
    df.dropna(inplace=True)
    print(f"Total Records: {len(df)}")
    for index, rows in df.iterrows():
        #print(f"[+]Product: {rows['prod-name']}, URL: {rows['prod-image-src']}")
        productName = rows['prod-name']
        tmpProdRef = firestoreDb.collection(u'latestprods').where('ProductName', '==', productName)
        docs = tmpProdRef.get()
        if docs:
            for doc in docs:
                tmpProdRef = firestoreDb.collection(u'latestprods').document(doc.id).update({
                    u'imageUrl': rows['prod-image-src']
                })
                print(f"[+] Updated Record: {doc.id} => {productName} with ImageUrl: {rows['prod-image-src']}")
                sys.stdout.flush()


def countTotalProducts():
    tmpProdRef = firestoreDb.collection(u'latestprods')
    docs = tmpProdRef.get()
    recordCount = len(list(docs))
    print(f"[=] Total records: {recordCount}")

def downloadImage(imageUrl, imgName):
    # urllib3.disable_warnings()
    url = imageUrl
    fileName = f"./ImgDump/{imgName}"
    # print (f"{fileName} exists: {os.path.isfile(fileName)}")
    # return
    if os.path.exists(fileName):
         return

    with urllib3.PoolManager() as http:

        r = http.request('GET', url)
        with open(fileName, 'wb') as fout:
            fout.write(r.data)
        print(f"[+]File downloaded: {fileName}")

    return

def processProdsForImgs():
    tmpProdRef = firestoreDb.collection(u'latestprods')
    docs = tmpProdRef.get()
    tmpDoc = []
    if docs:
        for doc in docs:
            docJson = doc.to_dict()
            prodName = docJson.get("ProductName", "noprodname")
            prodName = re.sub('[^a-zA-Z0-9 \n\.]', '', prodName)
            prodName = prodName.lower().replace("-","").replace(" ","-").replace(".","")
            imgUrl = docJson.get("imageUrl", "---EMPTY---")
            if imgUrl != "---EMPTY---":
                # downloadImage(imgUrl, f"{prodName}.png")
                tmpDoc.append({u"FireBaseObjID":doc.id, u"ProductName":prodName,u"ImageName":prodName+'.png', u"imageUrl":imgUrl})
            #print(f"[*] {doc.id} : {prodName}")
            # sys.stdout.flush()
    # addStrainsFromFile("StrainsSativaSorted.json")
    # backupProds = firestoreDb.collection(u'latestprods')
    # toCollectionRef = firestoreDb.collection(u'prods_backup')
    df = pandas.DataFrame.from_dict(tmpDoc)
    for index, row in df.iterrows():
        imgUrl = row['imageUrl']
        prodName = row['ProductName']
        try:
            downloadImage(imgUrl, f"{prodName}.png")
        except Exception as e:
            print(f"[-] {prodName}, {imgUrl} Error => {e}")

    print(f"[+] Total records: {len(tmpDoc)}")

def backupStrainsToJSON(fileName):
    tmpProdRef = firestoreDb.collection(u'strains2')
    docs = tmpProdRef.get()
    tmpDocs = []
    if docs:
        for doc in docs:
            tmpDocs.append({u'strain': doc.to_dict()})

    # data = json.loads(tmpDocs)
    df = pandas.DataFrame.from_dict(tmpDocs)
    # print(df)
    df.to_json(path_or_buf=fileName)
    print(f"[+]File dumped: {fileName}")
    # print(f"[+] Total records: {len(tmpDoc)}")

def backupProductsToJSON(fileName):
    tmpProdRef = firestoreDb.collection(u'latestprods')
    docs = tmpProdRef.get()
    tmpDocs = []
    if docs:
        for doc in docs:
            tmpDocs.append({u'product': doc.to_dict()})

    # data = json.loads(tmpDocs)
    df = pandas.DataFrame.from_dict(tmpDocs)
    # print(df)
    df.to_json(path_or_buf=fileName)
    print(f"[+]File dumped: {fileName}")


def excelToJson(filename):
    tmpFilename = filename
    tmpJsonName = filename.split('.')[0] + '.json'
    df = pandas.read_excel(tmpFilename)
    print(f"[+] Dumping {tmpFilename} to JSON: {tmpJsonName} ")
    df.to_json(tmpJsonName)
    print("[+] Done !")
def main():
    # backupStrainsToJSON('./Strains.json')
    # backupProductsToJSON('./Products.json')
    excelToJson('imageKey.xlsx')
    # processProdsForImgs()
    # deleteSativaStrain()

    # print("[!] Backing up, please wait...")
    # backupCollection(backupProds, toCollectionRef)

    # print("[+]Add Waven to latestprods ")
    # strainBckRef = firestoreDb.collection('latestprods')
    # # addAuthorNameToCollection(strainBckRef)
    # count = 0
    # docs = strainBckRef.where(u'UpdatedBy', u'==', u'Waven').get()
    # for doc in docs:
    #     count += 1
    # print("Latest prods size: {}".format(count))

    # prods = getProductsData()
    # dummyData = [{
    #     u'name': u'rohit'
    # },{
    #     u'name': u'kiran'
    # },{
    #     u'name': u'happy'
    # }]

    # dummyData = loadDataFromFile("./data/Cannabis_shake.json")
    #
    # results = searchInList('ProductName', 'Shells 28g', dummyData['Product'])
    # for doc in results:
    #     # print("[+] First product record: {}".format(doc))
    #     img = xtractImgFromObj(doc)
    #     print("[+] Image is => {}".format(img[2]))

    # uploadImgThumbsFromFolder("./thumbs")

    # updateImageOfProduct("Purple Trainwreck", "https://www.google.com")

    # loadImagesFromFile("./json_products/Topicals_lubricant-oils.json")
    # processImageJsonFiles("./json_products")


    # imgFileName = "Edibles_tinctures-sublingual.xlsx"
    # loadProdsFromXLSX(imgFileName)
    # os.rename(f"json_products\{imgFileName}", f"json_products\Done {imgFileName}")
    # countRecordsWithImages()
    # countTotalProducts()
    # getProductsWithoutImages()
    # delProductsWithoutImages()


if __name__ == '__main__':
    main()