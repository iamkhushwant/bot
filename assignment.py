from pymongo import MongoClient
import pymongo
import os
import time
import shutil
def get_db():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017"
    client = MongoClient(CONNECTION_STRING)
    return client["file_store"]

if sorted(os.listdir()) != sorted(['processed', 'processing', 'queue', 'yo.py','assignment.py']):
    os.mkdir('processing')
    os.mkdir('queue')
    os.mkdir('processed')

def update_db(file_name):
    result = update_db.files.find_one_and_update(
        {"file": file_name}, {"$set": {"processed": 1}}
    )
    if result == None:
        print("file missing from database")
        return
    print("updated file")

update_db.files = get_db()["files"]


def add_file_to_db(file_name):
    add_file_to_db.files.insert_one({"file": file_name, "processed": 0})
    print("added new file")


add_file_to_db.files = get_db()["files"]

def move(source,destination):
    # source = 'processing\\'
    # destination = 'queue\\'
    allfiles = os.listdir(source)
    for f in allfiles:
        shutil.move(source + f, destination + f)

t = 0
while t >= 0:
    with open(f'processing\\file{t}' , 'w') as file:
        file.write('here is your assignment.')
    add_file_to_db(f'file{t}')
    time.sleep(1)
    t += 1
    if t % 5 == 0:
        move('processing\\', 'queue\\')
        allfiles = os.listdir('queue\\')
        for f in allfiles:
            update_db(f)
            shutil.move('queue\\' + f, 'processed\\' + f)

