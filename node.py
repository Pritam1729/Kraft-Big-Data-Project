import threading
import time
import utils
import datetime
from utils import cfg

FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

class Node():
	def __init__(self, fellow, my_ip):
		self.addr = my_ip
		self.fellow = fellow
		self.lock = threading.Lock()
		self.DB = {}
		self.topics = {}
		self.topicids = {}
		self.log = []
		self.staged = None
		self.term = 0
		self.status = FOLLOWER
		self.majority = ((len(self.fellow) + 1) // 2) + 1
		self.voteCount = 0
		self.commitIdx = 0
		self.timeout_thread = None
		self.topicglobalId = 0
		self.brokerglobalId = 0
		self.partitionglobalId = 0
		self.prevtime = None
		self.clprevtime = None
		self.prodglobalId =0
		self.brokers = {}
		self.alive = set()
		self.partitions = {}
		self.init_timeout()

	
	def incrementVote(self):
		self.voteCount += 1
		if self.voteCount >= self.majority:
			print(f"{self.addr} becomes the leader of term {self.term}")
			self.status = LEADER
			self.startHeartBeat()


	def BeginElection(self):
		self.term += 1
		self.voteCount = 0
		self.status = CANDIDATE
		self.init_timeout()
		self.incrementVote()
		self.send_vote_req()

   
	def send_vote_req(self):
		
		for voter in self.fellow:
			threading.Thread(target=self.ask_vote,
							 args=(voter, self.term)).start()

	
	
		
	def send_heartbeat(self, follower):
		
		if self.log:
			self.update_follower(follower)

		route = "heartbeat"
		message = {"term": self.term, "addr": self.addr}
		while self.status == LEADER:
			start = time.time()
			reply = utils.send(follower, route, message)
			if reply:
				self.heartbeat_repl(reply.json()["term"],
											 reply.json()["commitIdx"])
			delta = time.time() - start
			
			time.sleep((cfg.HB_TIME - delta) / 1000)
	
	def init_timeout(self):
		self.reset_timeout()
		if self.timeout_thread and self.timeout_thread.is_alive():
			return
		self.timeout_thread = threading.Thread(target=self.timeout_loop)
		self.timeout_thread.start()



	def startHeartBeat(self):
		print("Starting HEARTBEAT")
		if self.staged:
			
			self.handle_put(self.staged)

		for each in self.fellow:
			t = threading.Thread(target=self.send_heartbeat, args=(each, ))
			t.start()

	def update_follower(self, follower):
		route = "heartbeat"
		first_message = {"term": self.term, "addr": self.addr}
		second_message = {
			"term": self.term,
			"addr": self.addr,
			"action": "commit",
			"payload": self.log[-1]
		}
		reply = utils.send(follower, route, first_message)
		if reply and reply.json()["commitIdx"] < self.commitIdx:
			
			reply = utils.send(follower, route, second_message)

	

	
	def heartbeat_repl(self, term,commitid):
		
		if term > self.term:
			self.term = term
			self.status = FOLLOWER
			self.init_timeout()

	def ask_vote(self, voter, term):
		
		message = {
			"term": term,
			"commitIdx": self.commitIdx,
			"staged": self.staged
		}
		route = "vote_req"
		while self.status == CANDIDATE and self.term == term:
			reply = utils.send(voter, route, message)
			if reply:
				choice = reply.json()["choice"]
				
				if choice and self.status == CANDIDATE:
					self.incrementVote()
				elif not choice:
					
					term = reply.json()["term"]
					if term > self.term:
						self.term = term
						self.status = FOLLOWER
					
				break    

	

	def reset_timeout(self):
		self.election_time = time.time() + utils.random_timeout()


	def heartbeat_follower(self, msg):
		
		term = msg["term"]
		if self.term <= term:
			self.leader = msg["addr"]
			self.reset_timeout()
			
			if self.status == CANDIDATE:
				self.status = FOLLOWER
			elif self.status == LEADER:
				self.status = FOLLOWER
				self.init_timeout()
			if self.term < term:
				self.term = term

			
			if "action" in msg:
				print("received action", msg)
				action = msg["action"]
				if action == "log":
					payload = msg["payload"]
					self.staged = payload
				elif self.commitIdx <= msg["commitIdx"]:
					if not self.staged:
						self.staged = msg["payload"]
					self.commit()

		return self.term, self.commitIdx
	
	def decide_vote(self, term, commitIdx, staged):
		
		if self.term < term and self.commitIdx <= commitIdx and (
				staged or (self.staged == staged)):
			self.reset_timeout()
			self.term = term
			return True, self.term
		else:
			return False, self.term


	

	def timeout_loop(self):
		
		while self.status != LEADER:
			delta = self.election_time - time.time()
			if delta < 0:
				self.BeginElection()
			else:
				time.sleep(delta)

	def handle_get(self, payload):
		print(self.DB)
		print("getting", payload)
		print("\n")
		key = payload["key"]
		if "param" in payload:
			param = payload["param"]
		if key in self.DB:
			if key == "TopicRecord":
				if "param" not in payload:
					payload["records"] = self.DB[key]["Records"]
				else:
					payload["records"] = self.DB[key]["Records"][self.topics[param]]
			elif key == "RegisterBrokerRecord":
				if "param" not in payload:
					payload["records"] = self.DB[key]["Records"]
				elif param == "Alive":
					payload["records"] = []
					for i in self.alive:
						payload["records"].append(self.DB[key]["Records"][self.brokers[i]])
				else:
					param = int(param)
					payload["records"] = self.DB[key]["Records"][self.brokers[param]]
			elif key == "PartitionRecord" or key == "ProducerIdRecord":
				payload["records"] = self.DB[key]["Records"]
			else:
				payload["value"] = self.DB[key]
			return payload
		elif key == "BrokerManagement":
			if self.prevtime == None:
				payload["key"] = "BrokerManagement"
				payload["records"] = []
				topicre = []
				for ele in self.topics:
					topicre.append(ele)
				# payload["records"].append({"key":"TopicRecord","records":self.topics["records"]})
				for i in self.DB.keys():
					temp = {}
					temp["key"] = i
					if i != None:
						temp["records"] = self.DB[i]
					payload["records"].append(temp)
					payload["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			else:
				present_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				prev_time = self.prevtime
				present_year,present_month,present_day = [int(part) for part in present_time.split(" ")[0].split("-")]
				present_hr,present_min,ok = [int(part) for part in present_time.split(" ")[1].split(":")]
				prev_year,prev_month,prev_day = [int(part) for part in prev_time.split(" ")[0].split("-")]
				prev_hr,prev_min,ok = [int(part) for part in prev_time.split(" ")[1].split(":")]
				
				payload["key"] = "BrokerManagement"
				payload["records"] = []
				payload["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				
				if present_year > prev_year or present_month > prev_month or present_day > prev_day or present_hr > prev_hr or present_min-prev_min >= 10:
					topicre = []
					for ele in self.topics:
						topicre.append(ele)
					# payload["records"].append({"key":"TopicRecord","records":self.topics["records"]})
					for i in self.DB.keys():
						temp = {}
						temp["key"] = i
						if i != "None":
							temp["records"] = self.DB[i]
						payload["records"].append(temp)
						payload["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				
				else:
					present_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
					for i in self.DB.keys():
						temp = {}
						temp["key"] = i
						temp["records"] = []
						if i != "None":
							temp = {}
							temp["key"] = i
							for record in self.DB[i]["Records"]:
								print(record)
								timestamp = datetime.datetime.strptime(record['Timestamp'], "%Y-%m-%d %H:%M:%S")
								pre_time = datetime.datetime.strptime(self.prevtime, "%Y-%m-%d %H:%M:%S")
								present_time = datetime.datetime.now()
								time_difference = present_time - timestamp
								time_difference2 = timestamp - pre_time
								re_temp = []
								if time_difference>= datetime.timedelta(minutes=0) and time_difference2 >= datetime.timedelta(minutes=0):
									re_temp.append(record)
							temp["records"] = re_temp
							payload["records"].append(temp)
							
					 
					
					
				
					
			self.prevtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			return payload
		elif key == "ClientManagement":
			if self.clprevtime == None:
				payload["key"] = "ClientManagement"
				payload["records"] = []
				topicre = []
				for ele in self.topics:
					topicre.append(ele)
				# payload["records"].append({"key":"TopicRecord","records":self.topics["records"]})
				for i in self.DB.keys():
					if i == "TopicRecord" or i == "PartitionRecord" or i == "RegisterBrokerRecord":
						temp = {}
						temp["key"] = i
						if i != None:
							temp["records"] = self.DB[i]
						payload["records"].append(temp)
						payload["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			else:
				present_time = datetime.datetime.now()
				prev_time = datetime.datetime.strptime(self.clprevtime, "%Y-%m-%d %H:%M:%S")

				payload = {"key": "ClientManagement", "records": [], "Timestamp": present_time.strftime("%Y-%m-%d %H:%M:%S")}

				if (
					present_time.year > prev_time.year
					or present_time.month > prev_time.month
					or present_time.day > prev_time.day
					or present_time.hour > prev_time.hour
					or present_time.minute - prev_time.minute >= 10
				):
					for i in self.DB.keys():
						if i == "TopicRecord" or i == "PartitionRecord" or i == "RegisterBrokerRecord":
							temp = {"key": i, "records": []}

							if i != "None":
									temp["records"] = self.DB[i]

							payload["records"].append(temp)

							payload["Timestamp"] = present_time.strftime("%Y-%m-%d %H:%M:%S")

				else:
					present_time = datetime.datetime.now()

					for i in self.DB.keys():
						if i == "TopicRecord" or i == "PartitionRecord" or i == "RegisterBrokerRecord":
							temp = {"key": i, "records": []}

							if i != "None":
									for record in self.DB[i]["Records"]:
										timestamp = datetime.datetime.strptime(record['Timestamp'], "%Y-%m-%d %H:%M:%S")
										pre_time = datetime.datetime.strptime(self.clprevtime, "%Y-%m-%d %H:%M:%S")

										time_difference = present_time - timestamp
										time_difference2 = timestamp - pre_time

										re_temp = []

										if time_difference >= datetime.timedelta(minutes=0) and time_difference2 >= datetime.timedelta(minutes=0):
								
												re_temp.append(record)

									temp["records"] = re_temp
									payload["records"].append(temp)
							
					 
					
			self.clprevtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			return payload
			
		else:
			return None


	def spread_update(self, message, confirmations=None, lock=None):
		for i, each in enumerate(self.fellow):
			r = utils.send(each, "heartbeat", message)
			if r and confirmations:
				confirmations[i] = True
		if lock:
			lock.release()

	def handle_put(self, payload):
		print("putting", payload)

		self.lock.acquire()
		self.staged = payload
		waited = 0
		log_message = {
			"term": self.term,
			"addr": self.addr,
			"payload": payload,
			"action": "log",
			"commitIdx": self.commitIdx
		}

		
		log_confirmations = [False] * len(self.fellow)
		threading.Thread(target=self.spread_update,
						 args=(log_message, log_confirmations)).start()
		while sum(log_confirmations) + 1 < self.majority:
			waited += 0.0005
			time.sleep(0.0005)
			if waited > cfg.MAX_LOG_WAIT / 1000:
				print(f"waited {cfg.MAX_LOG_WAIT} ms, update rejected:")
				self.lock.release()
				return False
		
		commit_message = {
			"term": self.term,
			"addr": self.addr,
			"payload": payload,
			"action": "commit",
			"commitIdx": self.commitIdx
		}
		t_id = self.commit()
		threading.Thread(target=self.spread_update,
						 args=(commit_message, None, self.lock)).start()
		print("majority reached, replied to client, sending message to commit")
		return t_id

		
	def commit(self):
		self.commitIdx += 1
		self.log.append(self.staged)
		key = self.staged["key"]
		retval = 0
		if key == "TopicRecord":
			if "op" in self.staged:
				param = self.staged["param"]
				if param not in self.topics:
					return -2
				idx = self.topics[param]
				det = None
				for i in self.topics.items():
					if i[1] == idx:
						det = i[0]
					if i[1] > idx:
						self.topics[i[0]] -= 1
				self.topics.pop(det)
				for i in self.topicids.items():
					if i[1] == idx:
						det = i[0]
					if i[1] > idx:
						self.topicids[i[0]] -= 1
				self.topicids.pop(det)
				self.DB[key]["Records"].pop(idx)
				return param
			name = self.staged["name"]
			if name in self.topics:
				return -1
			if key not in self.DB:
				self.DB[key] = {}
				self.DB[key]["Records"] = []
			temp = {}
			self.topics[name] = len(self.DB[key]["Records"])
			self.topicids[self.topicglobalId] = len(self.DB[key]["Records"])
			temp["topicUUID"] = self.topicglobalId
			self.topicglobalId += 1
			temp["name"] = name
			temp["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			self.DB[key]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			self.DB[key]["Records"].append(temp)
			print(self.DB[key])
			retval = self.topicglobalId-1

		elif key == "RegisterBrokerRecord":
			if "op" in self.staged:
				param = int(self.staged["param"])
				if param not in self.brokers:
					return -3
				idx = self.brokers[param]

				for i in self.brokers.items():
					if i[1] > idx:
						self.brokers[i[0]] -= 1
				self.brokers.pop(param)
				self.DB[key]["Records"].pop(idx)
				return param
			host = self.staged["addr"]
			status = self.staged["status"]
			port = self.staged["port"]

			if key not in self.DB:
				self.DB[key] = {}
				self.DB[key]["Records"] = []
			temp = {}
			self.brokers[self.brokerglobalId] = len(self.DB[key]["Records"])
			temp["brokerId"] = self.brokerglobalId
			temp["brokerHost"] = host
			temp["brokerStatus"] = status
			temp["epoch"] = 0
			temp["brokerPort"] = port
			temp["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			if status == "Alive":
				self.alive.add(self.brokerglobalId)
			self.brokerglobalId += 1
			self.DB[key]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			self.DB[key]["Records"].append(temp)
			print(self.DB[key])
			retval = self.brokerglobalId-1

		elif key == "PartitionRecord":
			if "op" in self.staged:
				op = self.staged["op"]
				partitionId = int(self.staged["partitionId"])
				brokerId = int(self.staged["brokerId"])

				if partitionId not in self.partitions:
					return -4
				elif brokerId not in self.brokers:
					return -3

				if op == "add":
					self.DB[key]["Records"][self.partitions[partitionId]]["addingReplicas"].append(brokerId)
					self.DB[key]["Records"][self.partitions[partitionId]]["replicas"].append(brokerId)
					self.DB[key]["Records"][self.partitions[partitionId]]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				else:
					if brokerId not in self.DB[key]["Records"][self.partitions[partitionId]]["replicas"]:
						return -3
					self.DB[key]["Records"][self.partitions[partitionId]]["removingReplicas"].append(brokerId)
					self.DB[key]["Records"][self.partitions[partitionId]]["replicas"].remove(brokerId)
					self.DB[key]["Records"][self.partitions[partitionId]]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
				self.DB[key]["Records"][self.partitions[partitionId]]["partitionEpoch"] += 1
				self.DB[key]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
				retval = brokerId
			else:
				topicId = int(self.staged["topicId"])
				brokerId = int(self.staged["brokerId"])

				if topicId not in self.topicids:
					return -2
				elif brokerId not in self.brokers:
					return -3

				if key not in self.DB:
					self.DB[key] = {}
					self.DB[key]["Records"] = []

				temp = {}
				temp["partitionId"] = self.partitionglobalId
				self.partitions[self.partitionglobalId] = len(self.DB[key]["Records"])
				temp["topicUUID"] = topicId
				temp["replicas"] = []
				temp["ISR"] = [i for i in self.brokers]
				temp["removingReplicas"] = []
				temp["addingReplicas"] = []
				temp["leader"] = brokerId
				temp["partitionEpoch"] = 0
				temp["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
				self.partitionglobalId += 1
				self.DB[key]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
				self.DB[key]["Records"].append(temp)
				retval = self.partitionglobalId-1

		elif key == "ProducerIdsRecord":
			brokerId = int(self.staged["brokerId"])

			if brokerId not in self.brokers:
				return -3
			if key not in self.DB:
				self.DB[key] = {}
				self.DB[key]["Records"] = []
			temp = {}
			temp["producerId"] = self.prodglobalId
			temp["brokerId"] = brokerId
			temp["brokerEpoch"] = self.DB["RegisterBrokerRecord"]["Records"][self.brokers[brokerId]]["epoch"]
			temp["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			self.prodglobalId += 1      
			self.DB[key]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
			self.DB[key]["Records"].append(temp)
			retval = self.prodglobalId-1

		elif key == "RegistrationChangeBrokerRecord":
			broker_id = self.staged["brokerId"]
			host = self.staged["brokerHost"]
			status = self.staged["brokerStatus"]
			port = self.staged["brokerPort"]
			protocol = self.staged["securityProtocol"]

			
			if "RegisterBrokerRecord" in self.DB and broker_id in self.brokers:
				idx = self.brokers[broker_id] 

				
				self.DB["RegisterBrokerRecord"]["Records"][idx]["brokerHost"] = host
				self.DB["RegisterBrokerRecord"]["Records"][idx]["brokerStatus"] = status
				self.DB["RegisterBrokerRecord"]["Records"][idx]["brokerPort"] = port

				self.DB["RegisterBrokerRecord"]["Records"][idx]["epoch"] += 1

				if status == "Alive":
					self.alive.add(broker_id)

				retval = broker_id 
			else:
				retval = -1

			print(self.DB["RegisterBrokerRecord"]["Records"])

		else:
			value = self.staged["value"]
			self.DB[key] = value
		
		self.staged = None
		return retval



