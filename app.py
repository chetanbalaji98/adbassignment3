import time
import os
from datetime import datetime,timedelta
import pyodbc
from flask import Flask, render_template, request, flash
from timeit import default_timer as timer 
import redis 
import pickle

app = Flask(__name__)
app.config["image_folder"] = "./static/"
app.config['UPLOAD_EXTENSIONS'] = ['jpg', 'png', 'gif']
app.secret_key = "Secret Key"


DRIVER = '{ODBC Driver 18 for SQL Server}'
SERVER = 'adbserver.database.windows.net'
DATABASE = 'chetanadb'
USERNAME = 'chetanbalaji'
PASSWORD = 'Springadb123'

cnxn = pyodbc.connect("Driver={};Server=tcp:{},1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;".format(DRIVER, SERVER, DATABASE, USERNAME, PASSWORD))
crsr = cnxn.cursor()

myhost="adbassignment3.redis.cache.windows.net"
mypassword="132sjl5xDwrHJ3uWeELOwpEVr7ZFEkQNVAzCaKl7NAQ="
r=redis.StrictRedis(host='{}'.format(myhost),port=6380, password='{}'.format(mypassword), ssl=True)
print(r.get("name"))


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/question14', methods=['GET', 'POST'])
def question14():
    n=str(request.form.get("n"))
    startpop = str(request.form.get("startpop"))
    endpop = str(request.form.get("endpop"))
    q14aquery = "select top {} City,State,Population from dbo.[data-5] where Population > '{}' and Population < '{}' order by Population desc".format(n,startpop,endpop)
    crsr.execute(q14aquery)
    result = crsr.fetchall()
    count = len(result)
    q14bquery = "select top {} City,State,Population from dbo.[data-5] where Population > '{}' and Population < '{}' order by Population".format(n,startpop,endpop)
    crsr.execute(q14bquery)
    result2 = crsr.fetchall()
    count2=len(result2)
    return render_template('question14.html',count=count, list1=result,list2=result2,count2=count2)

@app.route('/question1', methods=['GET', 'POST'])
def question1():

    starttime=timer()
    for i in range(3):
        q1query = "select * from dbo.all_month"
        crsr.execute(q1query)
        result = crsr.fetchall()
    endtime=timer()
    totaltime=endtime-starttime
    return render_template('index.html',totaltime=totaltime)

@app.route('/question2', methods=['GET', 'POST'])
def question2():
    if request.method == 'POST':
        sqlquery = "select date from dbo.all_month"
        hashvalue = "Chetan2"
        redis_key = "{}".format(hashvalue)
        list1=[]
        starttime=timer()
        for data in range(5):
            starttime_a = timer()
            if not (r.get(redis_key)):
                print("Caching from Database")
                crsr.execute(sqlquery)
                fetchlist = list(crsr.fetchall())
                r.set(redis_key, pickle.dumps(list(fetchlist)))
                
            else:
                print("Caching from redis")   
            endtime=timer()

            time_elapsed_foriteration="%.1f ms" % (1000 * (endtime - starttime_a))
            print(time_elapsed_foriteration)
            list1.append(time_elapsed_foriteration)

        time_elapsed = "%.1f ms" % (1000 * (timer() - starttime))
        list1.append(time_elapsed)
        print(list1)
    return render_template('question2.html', randomquerytimeredis = time_elapsed,list1=list1)

@app.route('/question3', methods=['GET', 'POST'])
def question3():
    if request.method == 'POST':
        
        hashvalue = "Chetan2"
        redis_key = "{}".format(hashvalue)
        list2=[]
        starttime=timer()
        for data in range(5):
            starttime_a = timer()
            r.get(redis_key)
            print("Caching from redis")   
            endtime=timer()
            time_elapsed_foriteration="%.1f ms" % (1000 * (endtime - starttime_a))
            print(time_elapsed_foriteration)
            list2.append(time_elapsed_foriteration)
            data=pickle.loads(r.get(redis_key))

        time_elapsed = "%.1f ms" % (1000 * (timer() - starttime))
        list2.append(time_elapsed)
        print(list2)
    return render_template('question3.html', randomquerytimeredis = time_elapsed,list2=list2)

if __name__ == "__main__":
    app.run(debug=True)