# dump_strains.py
# Author: Rohit Kaundal
# Date: 23 Feb 2019 10:32PM IST
#Description: This script connects to waven cloud storage and dumps strains data in chunks of 500 records

import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import csv
import pyfiglet
import time
import sys

creds = credentials.Certificate('./firestore-key.json') # Load API credentials key
fbApp = firebase_admin.initialize_app(creds) # Initialise firebase app

firestoreDb = firestore.client(fbApp) # Init firestore db

strainsRef = firestoreDb.collection(u'strains2') # get reference to strains db
prodsRef = firestoreDb.collection(u'latestprods') # get reference to products db

# read records from firestore reference with recordcount number of records
def get_data_async(firestoreRef, recordcount = 0): # async function returns single row at a time in the format {docid: value}
    data = []
    docs = ""
    if recordcount != 0:
        docs = firestoreRef.limit(recordcount).get() # Read all data from firestoreRef
    else:
        docs = firestoreRef.get()
    for doc in docs:
        data = ({doc.id: doc.to_dict()})
        yield  data

# Write json records to csv file
def write_strain_json_to_csv(filename, jsondata):

    with open(filename, 'w', encoding='utf-8') as f:
        csvfile = csv.writer(f) # open csv file for writing
        # Write header
        csvfile.writerow(['Strain Name', 'Strain Type', 'Ratings', 'Total Reviews', 'Strain Description'])
        # write rows now
        for row in jsondata:
            value = row.values()
            for val in value:
                csvfile.writerow([val.get('Name', "Empty"), val.get('Type', 'Not Defined'), val.get('Rating', 0), val.get('TotalReviews', 0), val.get('ProductDescription',"N/A")])


    print("[+] Filename : {} written with {} records".format(filename, len(jsondata)))
    sys.stdout.flush()

# Write json records to csv file
def write_product_json_to_csv(filename, jsondata):

    records_len = len(jsondata)

    with open(filename, 'w', encoding='utf-8') as f:
        csvfile = csv.writer(f) # open csv file for writing
        # Write header
        csvfile.writerow(['Product Name', 'Product Category', 'Ratings', 'Total Reviews', 'Description'])
        # write rows now
        for row in jsondata:
            value = row.values()
            for val in value:
                csvfile.writerow([val.get('ProductName', "Empty"), val.get('category_name', 'Not Defined'), val.get('StarRatings', 0), val.get('TotalReviews', 0), val.get('ProductDescription',"N/A")])


    print("[+] Filename : {} written with {} records".format(filename, len(jsondata)))
    sys.stdout.flush()




# Dump strain data
def DumpStrainData():
    data = []
    print("[+] Reading strains...")
    sys.stdout.flush()
    for rows in get_data_async(strainsRef):
       data.append(rows)
    print("[+] Dumping Strains to file[s]...")
    sys.stdout.flush()
    write_strain_json_to_csv("StrainsDump.csv", data)
    print("[+] Done !")
    sys.stdout.flush()

def DumpProductsData():
    data = []
    tmp_data = []
    records_per_file = 1000;
    file_count = 0;
    print("[+] Reading products...")
    sys.stdout.flush()
    for rows in get_data_async(prodsRef):
       data.append(rows)

    print("[+] Dumping Products to file[s]...")
    sys.stdout.flush()

    total_records = len(data)
    counter = 0
    for rows in data:
        tmp_data.append(rows)
        counter += 1
        if counter % 500 == 0:
            file_count += 1
            write_product_json_to_csv("ProductsDump-{}.csv".format(file_count), tmp_data)
            tmp_data.clear()
            sys.stdout.flush()

    print("[+] Done !")
    sys.stdout.flush()


def main():
    pyfiglet.print_figlet("WAVEN DB DUMPER", font = "Bubble")
    print("Author: Rohit Kaundal - Programmer / Tech Head ( Waven )")
    print("Date: 23 Feb 2019 10:32PM IST")
    print("="*35)
    doProcess = input("Start Dumping (Y/N): ")
    if doProcess.upper() == 'N':
        print("Aborting dumping...")
        time.sleep(3)
        return -1

    print("[+] Starting Waven CSV File Dumper")
    DumpStrainData()
    print("="*35)
    sys.stdout.flush()
    time.sleep(1)
    DumpProductsData()
    print("="*35)
    print("Dumping Complete !")
    sys.stdout.flush()
    time.sleep(1)

if __name__ == '__main__':
    main()
