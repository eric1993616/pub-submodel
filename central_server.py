import logging
from concurrent import futures
import time
import json
from datetime import datetime

import grpc
import pr_pb2
import pr_pb2_grpc
import _thread
import random
import sys
import socket
from pymongo import MongoClient
import time

CONNECTION_STRING = "mongodb+srv://ruihw95:89631139azrael@cluster0.cbxycjc.mongodb.net/?retryWrites=true&w=majority"

port = '50054'
SELF_IP = "localhost:" + str(port)
IS_MASTER = True
#two_phase_commit
def two_phase_init(request):
    channel = grpc.insecure_channel(centralServer)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    retries = 2
    while (retries>0) :
        try:
            response = stub.commit_request(request)
            if response.ack=="OK":
                logging.info("%s:%s:COMPLETE",str(datetime.now()),request.filename)
                return "COMPLETE"
            else :
                return "ERROR"

        except Exception as e:
            retries -= 1
            if retries==0:
                logging.info("%s:%s:Backup down, performing transaction...",str(datetime.now()),request.filename)
                print("Backup down...performing transaction")
                return "ERROR"

def roll_back(request):
    print("Roll back ...")
    #if pending insert then delete, if pending delete then insert
    documents = pending_twoLevelDict.find({"action":"remove"})
    for document in documents :
        twoLevelDict.insert_one({"topic":document["topic"],"publisher":document["publisher"],"subscriber":document["subscriber"]})

    documents = pending_twoLevelDict.find({"action":"insert"})
    for document in documents :
        twoLevelDict.delete_one({"topic":document["topic"],"publisher":document["publisher"],"subscriber":document["subscriber"]})
    documents = pending_frontends.find({"action":"remove"})
    for document in documents :
        typ = document["type"]
        frontends.insert_one({"type":document["type"],typ:document[typ]})
    documents = pending_frontends.find({"action":"insert"})
    for document in documents :
        typ = document["type"]
        frontends.delete_one({"type":document["type"],typ:document[typ]})
    pending_frontends.drop()
    pending_twoLevelDict.drop()

'''
gRPC central server 
'''
class CentralServer(pr_pb2_grpc.PublishTopicServicer):
    #commit_phase_one
    def commit_request(self, request, context):
        print('Start commit phase one ...')
        if request.filename == "twoLevelDict":
            if request.action == "remove":
                if request.level == "3":
                    documents = twoLevelDict.find({"topic": request.data_1, "subscriber": request.data_3})
                    for document in documents:
                        pending_twoLevelDict.insert_one({"topic": document["topic"], "publisher": document["publisher"],
                                                         "subscriber": document["subscriber"], "action": "remove"})
                    twoLevelDict.delete_one({"topic": request.data_1, "subscriber": request.data_3})
                    logging.info("%s:%s:REMOVE 3 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 "NIL", request.data_3)

                elif request.level == "2":
                    documents = twoLevelDict.find({"topic": request.data_1, "publisher": request.data_2})
                    for document in documents:
                        pending_twoLevelDict.insert_one({"topic": document["topic"], "publisher": document["publisher"],
                                                         "subscriber": document["subscriber"], "action": "remove"})
                    twoLevelDict.delete_many({"topic": request.data_1, "publisher": request.data_2})
                    logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 request.data_2, "NIL")

            elif request.action == "insert":
                if request.level == "3":
                    twoLevelDict.insert_one(
                        {"topic": request.data_1, "publisher": request.data_2, "subscriber": request.data_3})
                    documents = twoLevelDict.find(
                        {"topic": request.data_1, "publisher": request.data_2, "subscriber": request.data_3})
                    for document in documents:
                        pending_twoLevelDict.insert_one({"topic": document["topic"], "publisher": document["publisher"],
                                                         "subscriber": document["subscriber"], "action": "insert"})
                    logging.info("%s:%s:INSERT 3 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 request.data_2, request.data_3)

        elif request.filename == "frontends":
            if request.action == "remove":
                if request.level == "2":
                    if request.data_1 == "ip":
                        pending_frontends.insert_one(
                            {"type": request.data_1, request.data_1: request.data_2, "action": "remove"})
                        frontends.delete_one({"type": request.data_1, request.data_1: request.data_2})
                        logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)
                    else:
                        pending_frontends.insert_one(
                            {"type": request.data_1, request.data_1: int(request.data_2), "action": "remove"})
                        frontends.delete_one({"type": request.data_1, request.data_1: int(request.data_2)})
                        logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)

            elif request.action == "insert":
                if request.level == "2":
                    if request.data_1 == "ip":
                        pending_frontends.insert_one(
                            {"type": request.data_1, request.data_1: request.data_2, "action": "insert"})
                        frontends.insert_one({"type": request.data_1, request.data_1: request.data_2})
                        logging.info("%s:%s:INSERT 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)
                    else:
                        pending_frontends.insert_one(
                            {"type": request.data_1, request.data_1: int(request.data_2), "action": "insert"})
                        frontends.insert_one({"type": request.data_1, request.data_1: int(request.data_2)})
                        logging.info("%s:%s:INSERT 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)
        #commit_phase_two
        print('Commit phase one complete ...')
        retries = 0
        while (True):
            try:
                response = self.commit_phase_two(request, context)
                if response.ack == "COMMIT":
                    print('Commit phase two success!')
                    pending_frontends.drop()
                    pending_twoLevelDict.drop()
                    logging.info("%s:%s:COMMIT", str(datetime.now()), request.filename)
                    return pr_pb2.acknowledge(ack="OK")
                else:
                    return pr_pb2.acknowledge(ack="ERROR")
            except:
                retries += 1
                if retries > 3:
                    print("Central down, start roll back")
                    roll_back(request)
                    logging.info("%s:%s:ROLLBACK", str(datetime.now()), request.filename)
                    return pr_pb2.acknowledge(ack="ERROR")

    def commit_phase_two(self, request, context):
        print('Start commit phase two ...')
        if request.filename == "twoLevelDict":
            if request.action == "remove":
                if request.level == "3":
                    twoLevelDict.delete_one({"topic": request.data_1, "subscriber": request.data_3})
                    logging.info("%s:%s:REMOVE 3 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 "NIL", request.data_3)

                elif request.level == "2":
                    twoLevelDict.delete_many({"topic": request.data_1, "publisher": request.data_2})
                    logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 request.data_2, "NIL")

            elif request.action == "insert":
                if request.level == "3":
                    twoLevelDict.insert_one(
                        {"topic": request.data_1, "publisher": request.data_2, "subscriber": request.data_3})
                    logging.info("%s:%s:INSERT 3 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                 request.data_2, request.data_3)

        elif request.filename == "frontends":
            if request.action == "remove":
                if request.level == "2":
                    if request.data_1 == "ip":
                        frontends.delete_one({"type": request.data_1, request.data_1: request.data_2})
                        logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)
                    else:
                        frontends.delete_one({"type": request.data_1, request.data_1: int(request.data_2)})
                        logging.info("%s:%s:REMOVE 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)

            elif request.action == "insert":
                if request.level == "2":
                    if request.data_1 == "ip":
                        frontends.insert_one({"type": request.data_1, request.data_1: request.data_2})
                        logging.info("%s:%s:INSERT 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)
                    else:
                        frontends.insert_one({"type": request.data_1, request.data_1: int(request.data_2)})
                        logging.info("%s:%s:INSERT 2 %s %s %s", str(datetime.now()), request.filename, request.data_1,
                                     request.data_2, request.data_3)

        logging.info("%s:%s:COMMIT", str(datetime.now()), request.filename)
        return pr_pb2.acknowledge(ack="COMMIT")
    # register front end server ip to mongoDB
    def registerIp(self, request, context):
        print("Register front end server ip!\n")
        #upgrade two phase
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="2",data_1 = "ip",data_2=request.ip, data_3 = "", filename = "frontends",function_name="getFrontIp"))
            if response == "ERROR" :
                frontends.insert_one({"type":"ip","ip":request.ip})
        else :
            frontends.insert_one({"type":"ip","ip":request.ip})
        return pr_pb2.acknowledge(ack="Ip added...")

    # send registered front end server ip to client
    def getFrontIp(self, request, context):
        print("Sending front end server ip!\n")
        cursor = frontends.find({"type": "ip"})
        lst = []
        for document in cursor:
            lst.append(document["ip"])
        ip = random.choice(lst)
        return pr_pb2.ips(ip=ip)

    # send topic server ips to front end server
    def giveIps(self, request, context):
        print("Received topic!\n")
        # find a existing topic server without any subscribers
        find = {"topic": request.topic, "subscriber": "NULL"}
        if twoLevelDict.count_documents(find) > 0:
            for document in twoLevelDict.find(find):
                yield pr_pb2.ips(ip=document["publisher"])
        # sign a front server as a topic server if no empty topic server available
        else:
            cursor = frontends.find({"type": "ip"})
            lst = []
            for document in cursor:
                lst.append(document["ip"])
            ip = random.choice(lst)
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert", level="3", data_1=request.topic, data_2=ip, data_3 = "NULL", filename="twoLevelDict", function_name="giveIps"))
                if response == "ERROR":
                    twoLevelDict.insert_one({"topic": request.topic, "publisher": ip, "subscriber": "NULL"})
            else:
                twoLevelDict.insert_one({"topic": request.topic, "publisher": ip, "subscriber": "NULL"})
            yield pr_pb2.ips(ip=ip)

    # send topic subscriber ips to topic server
    def giveSubscriberIps(self, request, context):
        print("Give Subscribers ip!\n")
        find = {"topic": request.topic, "publisher": request.client_ip}
        # if only find 1 record with subscriber:null,. then no subscriber
        if twoLevelDict.count_documents(find) == 1:
            yield pr_pb2.ips(ip="none")
        # return subscriber ips
        else:
            cursor = twoLevelDict.find({"topic": request.topic, "publisher": request.client_ip})
            # yield pr_pb2.ips(ip='1234')
            for document in cursor:
                if document["subscriber"] != "NULL":
                    yield pr_pb2.ips(ip=document["subscriber"])

    # send available topics to client
    def querryTopics(self, request, context):
        print("Give available topics!\n")
        cursor = twoLevelDict.find({"subscriber": "NULL"})
        for document in cursor:
            yield pr_pb2.topic(topic=document["topic"])

    # add topic subscriber to mongodb
    def subscribeRequestCentral(self, request, context):
        print("Subscribe request from access point", request.client_ip, " for topic", request.topic, " of type :", request.type)
        # if the requesting front end server not send subscribe request before
        if request.type == "new":
            # check if the front end server is signed for the topic, if it is, return the front end server ip as topic server ip
            find = {"topic": request.topic, "publisher": request.client_ip}
            if twoLevelDict.count_documents(find) > 0:
                allotedServer = request.client_ip
            else:
                l = sys.maxsize
                # find the topic that don't have any subscriber
                cursor = twoLevelDict.find({"topic": request.topic, "subscriber": "NULL"})
                # find the topic that has minimum number of front server
                for document in cursor:
                    ip = document["publisher"]
                    find = {"topic": request.topic, "publisher": ip}
                    if twoLevelDict.count_documents(find) < l:
                        l = twoLevelDict.count_documents(find)
                        tempIp = ip

                allotedServer = tempIp

            # add subcriber to topic server
            #upgrate two phase
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=allotedServer, data_3 = request.client_ip, filename = "twoLevelDict",function_name="subscribeRequestCentral"))
                if response == "ERROR" :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})
            else :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})

        # if the requesting front end server already assigned as a topic server
        else:
            # find corresponding topic server for the topic
            document = twoLevelDict.find_one({"topic": request.topic, "subscriber": request.client_ip})
            allotedServer = document["publisher"]

        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequest(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    # client subscribed to topic reach the threshold, needs to replicate
    def replicaRequest(self, request, context):
        print("Get replication request!\n")
        document = twoLevelDict.find_one({"topic":request.topic,"subscriber":"NULL"})
        allotedServer = document["publisher"]
        find = {"topic":request.topic,"publisher":request.client_ip}
        if twoLevelDict.count_documents(find) > 0 :
            return pr_pb2.acknowledge(ack="Requesting front end server already a replica for "+request.topic)

        find = {"topic":request.topic,"subscriber":request.client_ip}
        if twoLevelDict.count_documents(find) > 0 :
            #upgrade two phase
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="3",data_1 = request.topic,data_2="", data_3 = request.client_ip, filename = "twoLevelDict",function_name="replicaRequest"))
                if response == "ERROR" :
                    twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
            else :
                twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        # upgrade two phase
        if IS_MASTER:
            response = two_phase_init(
                pr_pb2.commit_req_data(action="insert", level="3", data_1=request.topic, data_2=request.client_ip,
                                       data_3="NULL", filename="twoLevelDict", function_name="replicaRequest"))
            if response == "ERROR":
                twoLevelDict.insert_one({"topic": request.topic, "publisher": request.client_ip, "subscriber": "NULL"})
        else:
            twoLevelDict.insert_one({"topic": request.topic, "publisher": request.client_ip, "subscriber": "NULL"})

        if IS_MASTER:
            response = two_phase_init(
                pr_pb2.commit_req_data(action="insert", level="3", data_1=request.topic, data_2=request.client_ip,
                                       data_3=request.client_ip, filename="twoLevelDict",
                                       function_name="replicaRequest"))
            if response == "ERROR":
                twoLevelDict.insert_one(
                    {"topic": request.topic, "publisher": request.client_ip, "subscriber": request.client_ip})
        else:
            twoLevelDict.insert_one(
                {"topic": request.topic, "publisher": request.client_ip, "subscriber": request.client_ip})

        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequestReplica(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print("Replication for topic server : "+request.client_ip+" started...")
        return pr_pb2.acknowledge(ack="Requesting front end server "+request.client_ip+" made a replica of topic(backup sent) "+request.topic)

    # unsubscribe front server from topic server
    def unsubscribeRequestCentral(self, request, context):
        print("unsubscribe request from access point", request.client_ip, " for topic", request.topic)
        #upgrate two phase
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="3",data_1 = request.topic,data_2="", data_3 = request.client_ip, filename = "twoLevelDict",function_name="unsubscribeRequestCentral"))
            if response == "ERROR" :
                twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        else :
            twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    # dereplicate front server if subscriber for topic drops under threshold
    def deReplicaRequest(self, request, context):
        dct = {}
        cursor = twoLevelDict.find({"topic": request.topic})
        for document in cursor:
            if document["publisher"] in dct:
                pass
            else:
                dct[document["publisher"]] = []
            if document["subscriber"] != "NULL":
                dct[document["publisher"]].append(document["subscriber"])


        #print(dct)
        '''
        # can't dereplicate if only has one front server
        if len(dct.keys()) == 1:
            return pr_pb2.acknowledge(ack="ERROR")
        '''

        extraSubscribers = dct[request.client_ip]
        #upgrade two phase
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="2",data_1 = request.topic,data_2=request.client_ip, data_3 = "", filename = "twoLevelDict",function_name="deReplicaRequest"))
            if response == "ERROR" :
                twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        else :
            twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        del dct[request.client_ip]

        # resign the front servers
        for subscriber in extraSubscribers:
            l = sys.maxsize
            for ip in dct.keys():
                find = {"topic": request.topic, "publisher": ip}
                count = twoLevelDict.count_documents(find)
                if count < l:
                    l = count
                    tempIp = ip
            #upgrade two phase
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=tempIp, data_3 = subscriber, filename = "twoLevelDict",function_name="deReplicaRequest"))
                if response == "ERROR" :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})
            else :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})


        print("Dereplication for topic server : "+request.client_ip+" started...")
        return pr_pb2.acknowledge(ack="DONE")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(CentralServer(), server)
    server.add_insecure_port(SELF_IP)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig(filename=port + '.log', format='%(message)s', filemode='w', level=logging.DEBUG)
    selfIpDct = json.load(open("backup.txt", "r"))
    virtualServer = selfIpDct["VIRTUAL_SERVER_IP"]
    centralServer = selfIpDct["CENTRAL_SERVER_IP"]
    backupCentralServer = selfIpDct["BACKUP_SERVER_IP"]
    channel = grpc.insecure_channel(virtualServer)
    stub = pr_pb2_grpc.PublishTopicStub(channel)

    mongoClient = MongoClient(CONNECTION_STRING)
    mongoClient.drop_database('CentralServer' + port)
    db = mongoClient['CentralServer' + port]
    frontends = db["frontends"]
    twoLevelDict = db["twoLevelDict"]
    pending_twoLevelDict = db["pending_twoLevelDict"]
    pending_frontends = db["pending_frontends"]

    serve()
