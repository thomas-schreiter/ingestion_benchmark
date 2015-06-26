import sys
import os
sys.path.append(os.path.join('..', 'src'))

from flask import Flask, render_template
import dbwrapper
import prettytable
import string

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
    kafka_throughput =   [ dbwrapper.query_latest_throughput(  "kafka_%dprod" % i) for i in [1,4,16] ]
    kinesis_throughput = [ dbwrapper.query_latest_throughput("kinesis_%dprod" % i) for i in [1,4,16] ]

    return render_template('Bootstrap_hello.html', 
                           kafka_throughput=kafka_throughput,
                           kinesis_throughput=kinesis_throughput)

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
    
