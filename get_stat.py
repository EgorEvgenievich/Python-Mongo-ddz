import argparse
from pprint import pprint
from connect import Connect

def arg_parser():
	parser = argparse.ArgumentParser(description='display journal (по умолчанию как входной файл)')
	parser.add_argument("-avg", "--average", action='store_true', help="display average mark for [fullname] [subject]")
	parser.add_argument("-f", "--fullnames", action='append', help="display listener with name <fulname> marks")
	parser.add_argument("-s", "--subjects", action='append', help="display journal for <subject>")
	parser.add_argument("-p", action="append", help="display pz for subject")
	parser.add_argument("-j", "--json", action="store_true", help="display in JSON format")
	return parser.parse_args()

class Printer():
    def __init__(self, fullname, subject, pz, date, time, mark, maxmark, exec_time, ip_addr):
        self.fullname=fullname
        self.subject=subject
        self.pz=pz
        self.date=date
        self.time=time
        self.maxmark=maxmark
        self.mark=mark
        self.exec_time=exec_time
        self.ip_addr=ip_addr
    def __str__(self):
        return f"{self.fullname} {self.subject} {self.pz} {self.date} {self.time} {self.mark}/{self.maxmark} {self.exec_time} {self.ip_addr}"
class Storage():
    """Journal of marks"""
    def get_col(self):
        """Get collection"""
        self._client = Connect.get_connection()
        db = self._client["listners"]
        return db["listners"]

    def get_avg_mark(self, fullnames=None, subjects=None, json=False):
        """Get average mark"""
        col = self.get_col()
        if fullnames == None:
            fullnames = col.distinct("fullname")
        if subjects == None:
            subjects = col.distinct("subjects.subject")
        for fullname in fullnames:

            for subject in subjects:
                pipeline = [{"$match": {"fullname": fullname}}, {"$unwind": "$subjects"},
                            {"$match": {"subjects.subject": subject}},
                            {"$project": {"_id": 0, "fullname": 1, "subject": "$subjects.subject",
                                          "mark": "$subjects.mark", "maxmark": "$subjects.maxmark",
                                          "count": {"$add": [1]}}},
                            {"$group": {"_id": {"fullname": "$fullname", "subject": "$subject"},
                                        "average mark": {"$avg": "$mark"}}},

                           ]
                if json:
                    for cursor in col.aggregate(pipeline):
                        pprint(cursor)
                else:
                    for cursor in col.aggregate(pipeline):
                        print("Listner: {fullname}\tSubject: {subject}\tAverage Mark: {mark}".format(
                            fullname=cursor["_id"]["fullname"], subject=cursor["_id"]["subject"],
                            mark=cursor["average mark"]))

    def get_stat(self, fullnames=None, subjects=None, p=None, json=False):
        """Get stat"""
        col = self.get_col()
        if fullnames == None:
            fullnames = col.distinct("fullname")
        if fullnames and not subjects and not p:
            for fullname in fullnames:
                pipeline = [{"$match": {"fullname": fullname}}, {"$project":{"_id":0}}]
                if json:
                    for cursor in col.aggregate(pipeline):
                        pprint(cursor)
                else:
                    pipeline = [{"$match": {"fullname":fullname}},
                              {"$project": {"_id":0}}, {"$unwind":"$subjects"}]
                    for cursor in col.aggregate(pipeline):
                        subject=Printer(cursor["fullname"], cursor["subjects"]["subject"],
                            cursor["subjects"]["pz"], cursor["subjects"]["date"],
                            cursor["subjects"]["time"], cursor["subjects"]["mark"],
                            cursor["subjects"]["maxmark"], cursor["subjects"]["exec_time"],
                            cursor["subjects"]["ip_addr"])
                        print(subject)

            exit(0)
        if subjects and not p:
            for fullname in fullnames:
                for subject in subjects:
                    pipeline = [{"$match": {"fullname":fullname}}, {"$unwind":"$subjects"},
                                {"$match":{"subjects.subject":subject}}]
                    if json:
                        for cursor in col.aggregate(pipeline):
                            pprint(cursor)
                    else:
                        for cursor in col.aggregate(pipeline):
                            subject = Printer(cursor["fullname"], cursor["subjects"]["subject"],
                                              cursor["subjects"]["pz"], cursor["subjects"]["date"],
                                              cursor["subjects"]["time"], cursor["subjects"]["mark"],
                                              cursor["subjects"]["maxmark"], cursor["subjects"]["exec_time"],
                                              cursor["subjects"]["ip_addr"])
                            print(subject)

            exit(0)
        if subjects and p:
            for fullname in fullnames:
                for subject in subjects:
                    for pz in p:
                        pipeline = [{"$match": {"fullname":fullname}}, {"$unwind":"$subjects"},
                                {"$match": {"subjects.subject":subject, "subjects.pz":pz}}]
                        if json:
                            for cursor in col.aggregate(pipeline):
                                pprint(cursor)
                        else:
                            for cursor in col.aggregate(pipeline):
                                subject = Printer(cursor["fullname"], cursor["subjects"]["subject"],
                                                  cursor["subjects"]["pz"], cursor["subjects"]["date"],
                                                  cursor["subjects"]["time"], cursor["subjects"]["mark"],
                                                  cursor["subjects"]["maxmark"], cursor["subjects"]["exec_time"],
                                                  cursor["subjects"]["ip_addr"])
                                print(subject)
            exit(0)



def main():
    storage = Storage()
    args = arg_parser()
    if args.average:
        storage.get_avg_mark(args.fullnames, args.subjects, args.json)
        exit(0)
    storage.get_stat(fullnames=args.fullnames, subjects=args.subjects, p=args.p, json=args.json)

if __name__ == '__main__':
    main()
