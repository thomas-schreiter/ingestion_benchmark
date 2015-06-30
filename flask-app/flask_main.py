import sys
import os
sys.path.append(os.path.join('..', 'src'))

from flask import Flask, render_template, jsonify
import dbwrapper
import prettytable
import string
import datetime


app = Flask(__name__)

@app.route("/index")
def hello():
    return "Hello World!"


@app.route("/prod")
def prod():
    """ show whole ProducedMsg table on the web """ 
    query = "SELECT * FROM ProducedMsg;"
    tablestr = dbwrapper._query_pretty(query)
    result = string.replace(str(tablestr),'\n','<br>')
    return result


@app.route("/con")
def con():
    """ show whole ConsumedMsg table on the web """
    query = "SELECT * FROM ConsumedMsg;"
    tablestr = dbwrapper._query_pretty(query)
    result = string.replace(str(tablestr),'\n','<br>')
    return result


@app.route("/")
@app.route("/bootstrap_hello")
def bootstrap_hello():
    """ show a super simple example of a bootstrapped page """
    poolsize = [1, 2, 3, 4, 5, 6, 7, 8]
    kafka_throughput =   [ dbwrapper.query_latest_throughput(  "kafka_%dprod" % i) for i in poolsize ]
    kinesis_throughput = [ dbwrapper.query_latest_throughput("kinesis_%dprod" % i) for i in poolsize ]

    return render_template('Bootstrap_hello.html', 
                           kafka_throughput=kafka_throughput,
                           kinesis_throughput=kinesis_throughput)


@app.route("/get_throughput_1prod")
def get42():
    kafka = dbwrapper.query_latest_throughput("kafka_1prod")
    kinesis = dbwrapper.query_latest_throughput("kinesis_1prod")
    return jsonify(kafka=kafka, kinesis=kinesis) #datetime.datetime.now().strftime("%S00"), '30'


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
    
