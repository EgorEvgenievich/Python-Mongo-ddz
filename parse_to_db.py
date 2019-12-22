import pymongo
from connect import Connect
import argparse
from pprint import pprint
import json

def arg_parser():
    parser=argparse.ArgumentParser(description="This scrip parse file to data base")
    parser.add_argument("file",help="add filename you whont write rto data base")
    filename=parser.parse_args()
    return filename.file

def list2pz(result):
    keys = ["subject","pz", "date", "time", "mark","maxmark", "exec_time", "ip_addr"]
    if  not type(result[5])==int:
        mark,maxmark=result[5].split('/')
        result.pop(5)
        result.insert(5,int(mark))
        result.insert(6,int(maxmark))
    pz = {keys[i]: result[i+1] for i in range(len(keys))}
    return pz

# def list2subj(result):
#     pz=list2pz(result)
#     subject={result[1]:[pz]}
#     return subject

def list2obj(result):
    pz=list2pz(result)
    obj={"fullname":result[0],"subjects":[pz]}
    return obj


def main():
    client=Connect.get_connection()
    db=client["listners"]
    col = db["listners"]
    filename=arg_parser()
    with open(filename,'r') as fd:
        lines=fd.read().splitlines()
    for line in lines:
        list=line.split()
        dict=list2obj(list)
        query={"fullname":list[0]}
        if  col.find_one(query):
            query1={"fullname":list[0],"subjects.subject":list[1],"subjects.pz":list[2]}
            if not col.find_one(query1):
                col.update_one(query,{"$push":{"subjects":list2pz(list)}})
        else:
            col.insert_one(dict)
    for x in col.find():
        pprint(x)
    pprint(col.count())
    query={"fullname":{"$in":["2018_3_05_Petrov","2018_3_06_Kuznetcov","2018_3_04_Ivanov"]}}
    #col.remove(query)

if __name__ == '__main__':
    main()
