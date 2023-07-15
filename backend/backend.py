from flask import Flask, request, abort, jsonify
import dataInput
# from sendToLine import sendToLine

app = Flask(__name__)

@app.route('/receiveMsg', methods=["GET","POST"])
def recieveMsg():
    data = request.get_json()
    # sendToLine(msg=data['status'],id=data['id']).push_msg()
    dataInput.dataInput(data=data['data'],id=data['id']).execute()
    return jsonify({"status":"ok"})

@app.route('/test', methods=["GET","POST"])
def test():
    data = request.get_json()
    # sendToLine(msg=data['status'],id=data['id']).push_msg()
    return jsonify({"status":"ok"})

if __name__ == "__main__":
    app.run(host="172.20.10.11",port='8000')
    #app.run(host="192.168.137.179",port='8000')



