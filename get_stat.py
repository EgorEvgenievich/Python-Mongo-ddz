import pymongo
import argparse
from pprint import pprint
from connect import Connect

def get_nobody():
    nobody = {"fullname": "2018_3_03_Nobody",
              "subjects": [{"timp": [
                  {"pz": "pz10", "mark": 7, "maxmark": 10, "exec_time": "9 minuts 10 seconds", "ip": "192.168.115.100"},
                  {"pz": "pz9", "mark": 8, "maxmark": 10, "exec_time": "9 minuts 10 seconds",
                   "ip": "192.168.115.100"}]},
                           {"bos": [{"pz": "pz10", "mark": 8, "maxmark": 10, "exec_time": "9 minuts 10 seconds",
                                     "ip": "192.168.115.100"}]},
                           {"Python": [{"pz": "pz10", "mark": 6, "maxmark": 10, "exec_time": "9 minuts 10 seconds",
                                        "ip": "192.168.115.100"}]}
                           ]
              }
    return nobody

def arg_parser():
	parser = argparse.ArgumentParser(description =' display journal (по умолчанию как входной файл)')
	parser.add_argument("-f","--fullname",action='append',help="display listener with name <fulname> marks")
	parser.add_argument("-s","--subject",action='append',help="display journal for <subject>")
	parser.add_argument("-p",action='append',help="display pz for subject ")
	parser.add_argument("-j","--json",help="display in JSON format")
	return parser.parse_args()

class Storage():

    def get_col(self):
        self._client=Connect.get_connection()
        db = self._client["listners"]
        return db["listners"]

    def get_stat(self,fullnames=None,subjects=None,p=None):
        col=self.get_col()
        if fullnames ==None:
            fullnames=col.distinct("fullname")
        if fullnames and not subjects and not p:
            for fullname in fullnames:
                pipeline = [{"$match": {"fullname": fullname}}]
                print(pipeline)
                for x in col.aggregate(pipeline):
                    pprint(x)
        if subjects and not p:
            for subject in subjects:
                pipeline = [{"$unwind": "$subjects"},{"$unwind": "$subjects."+subject}]
                for x in col.aggregate(pipeline):
                    pprint(x)
            exit(0)
        if subjects and p:
            for subject in subjects:
                for pz in p:
                    pipeline = [{"$unwind": "$subjects"}, {"$unwind": "$subjects." + subject},
                                {"$match": {"subjects." + subject + ".pz": pz}}]
                for x in col.aggregate(pipeline):
                    pprint(x)
            exit(0)
        for x in col.find():
            pprint(x)

    def get_avg_mark(self, fullnames=None, subjects=None ):
        col=self.get_col()
        if(fullnames==None):
            fullnames=col.distinct("fullname")
        for fullname in fullnames :
            if subjects==None:
               subjects=dict(col.distinct("subjects")).keys()
            for subject in subjects:
                pipeline=[{"$unwind":"$subjects."+subject}
                    ,{"$project":{"_id":0,"mark":1,"count":{"$add":[1]}}}
                    ,{"$group":{"total":{"$sum":"mark"},"amount":{"$sum":"count"}}}
                    ,{"$project":{"total":1,"amount":1}}]
                obj=dict(col.aggregate(pipeline))
                print("average is",obj["total"]/obj["amount"])


def main():
    args= arg_parser()
    print(args.subject)

main()
# dict(args).
# if len(args.fullname) and not len(args.p) and not len(args.p) :
#     for fullname in args.fullname:
#         pipeline=[{"$match":{"fullname":fullname}}]
#         print(pipeline)
#         for x in col.aggregate(pipeline):
#             pprint(x)
#     exit(0)
# if args.subject:
#     for subject in args.subject:
#         for pz in args.p:
#             pipeline=[{"$unwind":"$subjects"},{"$unwind":"$subjects."+subject}]
#         for x in col.aggregate(pipeline):
#             pprint(x)
#     exit(0)
# if args.p :
#     for subject in args.subject:
#         for pz in args.p:
#             pipeline=[{"$unwind":"$subjects"},{"$unwind":"$subjects."+subject},
#                 {"$match":{"subjects."+subject+".pz":pz}}]
#         for x in col.aggregate(pipeline):
#             pprint(x)
#     exit(0)
# for x in col.find():
#     pprint(x)
# print(type(args))
