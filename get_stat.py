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
	parser.add_argument("-j","--json",action="store_true",help="display in JSON format")
	return parser.parse_args()

class Storage():

    def get_col(self):
        self._client=Connect.get_connection()
        db = self._client["listners"]
        return db["listners"]

    def get_avg_mark(self, fullnames=None, subjects=None, json=False):
        client = Connect().get_connection()
        db = client["listners"]
        col = db["listners"]
        if fullnames == None:
            fullnames = col.distinct("fullname")
        if subjects == None:
            subjects = col.distinct("subjects.subject")
        for fullname in fullnames:

            for subject in subjects:
                pipeline = [{"$match": {"fullname": fullname}}, {"$unwind": "$subjects"},
                            {"$match": {"subjects.subject": subject}},
                            {"$project": {"_id": 0, "fullname": 1, "subject": "$subjects.subject",
                                          "mark": "$subjects.mark"
                                , "maxmark": "$subjects.maxmark", "count": {"$add": [1]}}},
                            {"$group": {"_id": {"fullname": "$fullname", "subject": "$subject"},
                                        "average mark": {"$avg": "$mark"}}},

                           ]
                if json:
                    for x in col.aggregate(pipeline):
                        pprint(x)
                else:
                    for x in col.aggregate(pipeline):
                        print("Listner: {fullname}\tSubject: {subject}\tAverage Mark: {mark}".format(
                            fullname=x["_id"]["fullname"],subject=x["_id"]["subject"],mark=x["average mark"]))

    def get_stat(self,fullnames=None,subjects=None,p=None,json=False):
        col=self.get_col()
        if fullnames ==None:
            fullnames=col.distinct("fullname")
        if fullnames and not subjects and not p:
            for fullname in fullnames:
                pipeline = [{"$match": {"fullname": fullname}},{"$project":{"_id":0}}]
                if json:
                    for x in col.aggregate(pipeline):
                        pprint(x)
                else:
                    pipeline=[{"$match": {"fullname": fullname}},{"$project":{"_id":0}},{"$unwind":"$subjects"}]
                    for x in col.aggregate(pipeline):
                        print("{0} {1} {2} {3} {4} {5} {6} {7} {8}".format(
                            x["fullname"],x["subjects"]["subject"],x["subjects"]["pz"],x["subjects"]["date"],
                            x["subjects"]["time"], x["subjects"]["mark"],x["subjects"]["maxmark"],
                            x["subjects"]["exec_time"],x["subjects"]["ip_addr" ]))

            exit(0)
        if subjects and not p:
            for fullname in fullnames:
                for subject in subjects:
                    pipeline = [{"$match": {"fullname":fullname}},{"$unwind":"$subjects"},
                                {"$match":{"subjects.subject":subject}}]
                    if json:
                        for x in col.aggregate(pipeline):
                            pprint(x)
                    else:
                        for x in col.aggregate(pipeline):
                            print("{0} {1} {2} {3} {4} {5} {6} {7} {8}".format(
                                x["fullname"], x["subjects"]["subject"], x["subjects"]["pz"], x["subjects"]["date"],
                                x["subjects"]["time"], x["subjects"]["mark"], x["subjects"]["maxmark"],
                                x["subjects"]["exec_time"], x["subjects"]["ip_addr"]))

            exit(0)
        if subjects and p:
            for fullname in fullnames:
                for subject in subjects:
                    for pz in p:
                        pipeline = [{"$match": {"fullname":fullname}},{"$unwind":"$subjects"},
                                {"$match":{"subjects.subject":subject,"subjects.pz":pz}}]
                        if json:
                            for x in col.aggregate(pipeline):
                                pprint(x)
                        else:
                            for x in col.aggregate(pipeline):
                                print("{0} {1} {2} {3} {4} {5} {6} {7} {8}".format(
                                    x["fullname"], x["subjects"]["subject"], x["subjects"]["pz"], x["subjects"]["date"],
                                    x["subjects"]["time"], x["subjects"]["mark"], x["subjects"]["maxmark"],
                                    x["subjects"]["exec_time"], x["subjects"]["ip_addr"]))
            exit(0)



def main():
    args= arg_parser()
    storage=Storage()
    args=arg_parser()
    if args.average:
        storage.get_avg_mark(args.fullnames,args.subjects,args.json)
        exit(0)
    storage.get_stat(fullnames=args.fullnames,subjects=args.subjects,p=args.p,json=args.json)
main()

