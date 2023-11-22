import sys, requests

def redirectToLeader(server_address, message):
    type = message["type"]
    while True:
        if type == "get":
            try:
                response = requests.get(server_address,
                                        json=message,
                                        timeout=1)
            except Exception as e:
                return e
        else:
            try:
                response = requests.put(server_address,
                                        json=message,
                                        timeout=1)
            except Exception as e:
                return e

        if response.status_code == 200 and "payload" in response.json():
            payload = response.json()["payload"]
            if "message" in payload:
                server_address = payload["message"] + "/request"
            else:
                break
        else:
            break
    return response.json()
    


def put(addr, key, value):
    server_address = addr + "/request"
    payload = {'key': key, 'value': value}
    message = {"type": "put", "payload": payload}
    print(redirectToLeader(server_address, message))



def get(addr, key):
    server_address = addr + "/request"
    payload = {'key': key}
    message = {"type": "get", "payload": payload}
    print(redirectToLeader(server_address, message))

def topic_record(addr, key, name):
	server_address = addr + "/request"
	payload = {"key": key, "name": name}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))

def broker_record(addr, key, status, port):
	server_address = addr + "/request"
	payload = {'addr': addr, 'key': key, "status": status, "port": port}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))
	
def getN(addr, key, param):
	server_address = addr + "/request"
	payload = {'key': key, "param": param}
	message = {"type": "get", "payload": payload}
	print(redirectToLeader(server_address, message))

def delete(addr, op, key, param):
	server_address = addr + "/request"
	payload = {'op': op,'key': key, "param": param}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))
		
def partition_record(addr, key, topicId, brokerId):
	server_address = addr + "/request"
	payload = {'key': key, "topicId":topicId, "brokerId": brokerId}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))
	
def partition_record2(addr, key, op, partitionId, brokerId):
	server_address = addr + "/request"
	payload = {'key': key, "op": op, "partitionId": partitionId, "brokerId": brokerId}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))

def producer_record(addr, key, brokerId):
	server_address = addr + "/request"
	payload = {'key': key, "brokerId": brokerId}
	message = {"type": "put", "payload": payload}
	print(redirectToLeader(server_address, message))
	


def broker_registration_change(addr,key, broker_id, broker_host, broker_port, security_protocol, broker_status):
    server_address = addr + "/request"
    payload = {
    	"key": key,
        "brokerId": broker_id,
        "brokerHost": broker_host,
        "brokerPort": broker_port,
        "securityProtocol": security_protocol,
        "brokerStatus": broker_status
        
    }
    message = {"type": "put", "payload": payload}
    print(redirectToLeader(server_address, message))	

	
if __name__ == "__main__":
    if len(sys.argv) == 3:
        addr = sys.argv[1]
        key = sys.argv[2]
        get(addr, key)
    elif sys.argv[2] == "delete":
        addr = sys.argv[1]
        op = sys.argv[2]
        key = sys.argv[3]
        param = sys.argv[4]
        delete(addr, op, key, param)
    elif sys.argv[2] == "get":
        addr = sys.argv[1]
        key = sys.argv[3]
        if len(sys.argv) == 4:
            get(addr, key)
        else:
            param = sys.argv[4]
            getN(addr, key, param)
    elif sys.argv[3] == "RegistrationChangeBrokerRecord":
        if len(sys.argv) != 9:
            print("Usage: python3 client.py <address> RegistrationChangeBrokerRecord <broker_id> <new_broker_host> <broker_port> <security_protocol> <broker_status> <epoch>")
        else:
            key = sys.argv[3]
            addr = sys.argv[1]
            broker_id = int(sys.argv[4])
            new_broker_host = sys.argv[5]
            broker_port = sys.argv[6]
            security_protocol = sys.argv[7]
            broker_status = sys.argv[8]
            broker_registration_change(addr, key, broker_id, new_broker_host, broker_port, security_protocol, broker_status)
    elif sys.argv[3] == "TopicRecord":
        addr = sys.argv[1]
        key = sys.argv[3]
        name = sys.argv[4]
        topic_record(addr, key, name)
    elif sys.argv[3] == "RegisterBrokerRecord":
        addr = sys.argv[1]
        key = sys.argv[3]
        status = sys.argv[4]
        port = sys.argv[5]
        broker_record(addr, key, status, port)
    elif sys.argv[3] == "PartitionRecord":
        addr = sys.argv[1]
        key = sys.argv[3]
        if sys.argv[4] == "add" or sys.argv[4] == "rem":
            op = sys.argv[4]
            partitionId = sys.argv[5]
            brokerId = sys.argv[6]
            partition_record2(addr, key, op, partitionId, brokerId)
        else:
            topicId = sys.argv[4]
            brokerId = sys.argv[5]
            partition_record(addr, key, topicId, brokerId)
    elif sys.argv[3] == "ProducerIdsRecord":
        addr = sys.argv[1]
        key = sys.argv[3]
        brokerId = sys.argv[4]
        producer_record(addr, key, brokerId)
    elif len(sys.argv) == 4:
        addr = sys.argv[1]
        key = sys.argv[2]
        val = sys.argv[3]
        put(addr, key, val)
    
