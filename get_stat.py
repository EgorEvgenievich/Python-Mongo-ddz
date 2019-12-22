import pymongo
import argparse
from pprint import pprint
from connect import Connect

def arg_parser():
	parser = argparse.ArgumentParser(description =' display journal (по умолчанию как входной файл)')
	parser.add_argument("-avg","--average",action='store_true',help="display average mark for [fullname] [subject]")
	parser.add_argument("-f","--fullnames",action='append',help="display listener with name <fulname> marks")
	parser.add_argument("-s","--subjects",action='append',help="display journal for <subject>")
	parser.add_argument("-p",action='append',help="display pz for subject")
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
                for x in col.aggregate(pipeline):
                    pprint(x)
            exit(0)
        if subjects and not p:
            for fullname in fullnames:
                for subject in subjects:
                    pipeline = [{"$match": {"fullname":fullname}},{"$unwind":"$subjects"},
                                {"$match":{"subjects.subject":subject}}]
                    for x in col.aggregate(pipeline):
                        pprint(x)
            exit(0)
        if subjects and p:
            for fullname in fullnames:
                for subject in subjects:
                    for pz in p:
                        pipeline = [{"$match": {"fullname":fullname}},{"$unwind":"$subjects"},
                                {"$match":{"subjects.subject":subject,"subjects.pz":pz}}]
                        for x in col.aggregate(pipeline):
                            pprint(x)
            exit(0)

    def get_avg_mark(self, fullnames=None, subjects=None ):
        client=Connect().get_connection()
        db=client["listners"]
        col=db["listners"]
        if fullnames==None:
            fullnames=col.distinct("fullname")
        if subjects==None:
            subjects=col.distinct("subjects.subject")
        for fullname in fullnames:
            for subject in subjects:
                pipeline=[{"$match":{"fullname":fullname}},{"$unwind":"$subjects"},
                          {"$match":{"subjects.subject":subject}},
                          {"$project":{"_id":0,"fullname":1,"subject":"$subjects.subject","mark":"$subjects.mark"
                                        ,"maxmark":"$subjects.maxmark","count":{"$add":[1]}}},
                         {"$group": {"_id":{"fullname":"$fullname","subject":"$subject"},
                          "average mark": {"$avg":"$mark"}}}
                ]
                for x in col. aggregate(pipeline):
                    pprint(x)
                # pprint(col.aggregate(pipeline))

def main():
    args= arg_parser()
    storage=Storage()
    args=arg_parser()
    if args.average:
        storage.get_avg_mark(args.fullnames,args.subjects)
        exit(0)
    pprint(args)
    storage.get_stat(fullnames=args.fullnames,subjects=args.subjects,p=args.p)
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
