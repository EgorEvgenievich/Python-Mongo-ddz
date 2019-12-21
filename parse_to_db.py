import pymongo
from connect import Connect
import argparse
from pprint import pprint

def arg_parser():
    parser=argparse.ArgumentParser(description="This scrip parse file to data base")
    parser.add_argument("file",help="add filename you whont write rto data base")
    filename=parser.parse_args()
    return filename.file

def list2pz(result):
    keys = ["pz", "date", "time", "mark","maxmark", "exec_time", "ip_addr"]
    if '/' in result[5]:
        mark,maxmark=result[5].split('/')
        int(mark)
        int(maxmark)
        result.pop(5)
        result.insert(5,mark)
        result.insert(6,maxmark)
    pz = {keys[i]: result[i+2] for i in range(len(keys))}
    print(pz)
    return pz

def list2subj(result):
    pz=list2pz(result)
    subject={result[1]:[pz]}
    return subject

def list2obj(result):
    subject=list2subj(result)
    obj={"fullname":result[0],"subjects":[subject]}
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
        print(list[5])
        dict=list2obj(list)
        query={"fullname":list[0]}
        if  col.find_one(query):
            query2={"fullname":list[0],"subjects."+list[1]:{"$exists":True}}
            if col.find_one(query2):
                #col.update_one({"fullname":list[0],"subjects.timp":{"$exists":1}},{"$push":{"subjects":{"timp":list2pz(list)}}})
                col.update_one(query2,{"$push":{"subjects":{list[1]:list2pz(list)}}})
            else:
                print("I there")
                col.update_one(query,{"$push":{"subjects":list2subj(list)}})
        else:
            col.insert_one(dict)
    for x in col.find():
        pprint(x)
    pprint(col.count())
    query={"fullname":{"$in":["2018_3_05_Petrov","2018_3_06_Kuznetcov","2018_3_04_Ivanov"]}}
    col.remove(query)
if __name__ == '__main__':
    main()
