from node import Node
from node import FOLLOWER, LEADER
from flask import Flask, request, jsonify
import sys
import logging

app = Flask(__name__)


@app.route("/request", methods=['GET'])
def value_get():
    payload = request.json["payload"]
    reply = {"code": 'fail', 'payload': payload}
    if n.status == LEADER:
        result = n.handle_get(payload)
        if result:
            reply = {"code": "success", "payload": result}
    elif n.status == FOLLOWER:
        reply["payload"]["message"] = n.leader
    return jsonify(reply)

@app.route("/request", methods=['PUT'])
def value_put():
    payload = request.json["payload"]
    reply = {"code": 'fail'}

    if n.status == LEADER:
        result = n.handle_put(payload)
        if result == -1:
            reply = {"code": "fail", "reason": "Entry already exists!"}
        elif result == -2:
            reply = {"code": "fail", "reason": "Topic does not exist!"}
        elif result == -3:
            reply = {"code": "fail", "reason": "Broker does not exist!"}
        elif result == -4:
            reply = {"code": "fail", "reason": "Partition does not exist!"}
        else:
            reply = {"code": "success", "id": result}
    elif n.status == FOLLOWER:
        payload["message"] = n.leader
        reply["payload"] = payload
    return jsonify(reply)

@app.route("/vote_req", methods=['POST'])
def vote_req():
    term = request.json["term"]
    commitIdx = request.json["commitIdx"]
    staged = request.json["staged"]
    choice, term = n.decide_vote(term, commitIdx, staged)
    message = {"choice": choice, "term": term}
    return jsonify(message)

@app.route("/heartbeat", methods=['POST'])
def heartbeat():
    term, commitIdx = n.heartbeat_follower(request.json)
    message = {"term": term, "commitIdx": commitIdx}
    return jsonify(message)


log = logging.getLogger('werkzeug')
log.disabled = True

if __name__ == "__main__":
    
    if len(sys.argv) == 3:
        index = int(sys.argv[1])
        ip_list_file = sys.argv[2]
        ip_list = []
        with open(ip_list_file) as f:
            for ip in f:
                ip_list.append(ip.strip())
        my_ip = ip_list.pop(index)

        http, host, port = my_ip.split(':')
        n = Node(ip_list, my_ip)
        app.run(host="0.0.0.0", port=int(port), debug=False)
    else:
        print("usage: python server.py <index> <ip_list_file>")
