import time
import os
from datetime import datetime,timedelta
import pyodbc
from flask import Flask, render_template, request, flash
from timeit import default_timer as timer 
import redis 
import pickle
import random

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


@app.route('/question10CBD', methods=['GET', 'POST'])
def question10CBD():
    starttime=timer()
    state=str(request.form.get("state"))
    startrank = str(request.form.get("startrank"))
    endrank = str(request.form.get("endrank"))
    q14aquery = "select City,State,Rank,Population from dbo.data3 where Rank >= {} and Rank < {} and State ='{}' ".format(startrank,endrank,state)
    crsr.execute(q14aquery)
    result = crsr.fetchall()
    count = len(result)
    endtime=timer()
    time_elapsed="%.1f ms" % (1000 * (endtime - starttime))


   
    return render_template('question10CBD.html',count=count, list1=result,time_elapsed=time_elapsed)


@app.route('/question11CBD', methods=['GET', 'POST'])
def question11CBD():
    state=str(request.form.get("state"))
    startrank = str(request.form.get("startrank"))
    endrank = str(request.form.get("endrank"))
    t=int(request.form.get("T"))
    if request.method == 'POST':
        sqlquery = "select City,State,Rank,Population from dbo.data3 where Rank >= {} and Rank < {} and State ='{}' ".format(startrank,endrank,state)
        hashvalue = "ChetanBalajiQuiz3"
        redis_key = "{}".format(hashvalue)
        list1=[]
        starttime=timer()
        for data in range(0,t):
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
    return render_template('question11CBD.html', randomquerytimeredis = time_elapsed,list1=list1)


@app.route('/question12CBD', methods=['GET', 'POST'])
def question12CBD():
    if request.method == 'POST':
        t=int(request.form.get("T"))
        hashvalue = "ChetanBalajiQuiz3"
        redis_key = "{}".format(hashvalue)
        list2=[]
        starttime=timer()
        for data in range(0,t):
            starttime_a = timer()
            if r.get(redis_key):
                print("Caching from redis")   
                endtime=timer()
                time_elapsed_foriteration="%.1f ms" % (1000 * (endtime - starttime_a))
                print(time_elapsed_foriteration)
                list2.append(time_elapsed_foriteration)
                data=pickle.loads(r.get(redis_key))

        time_elapsed = "%.1f ms" % (1000 * (timer() - starttime))
        list2.append(time_elapsed)
        print(list2)
    return render_template('question12CBD.html', randomquerytimeredis = time_elapsed,list2=list2)



@app.route('/question10CBD', methods=['GET', 'POST'])
def question10CBD():
    starttime=timer()
    state=str(request.form.get("state"))
    startrank = str(request.form.get("startrank"))
    endrank = str(request.form.get("endrank"))
    q14aquery = "select City,State,Rank,Population from dbo.data3 where Rank >= {} and Rank < {} and State ='{}' ".format(startrank,endrank,state)
    crsr.execute(q14aquery)
    result = crsr.fetchall()
    count = len(result)
    endtime=timer()
    time_elapsed="%.1f ms" % (1000 * (endtime - starttime))


   
    return render_template('question10CBD.html',count=count, list1=result,time_elapsed=time_elapsed)





































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
    for i in range(100):
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

@app.route('/question10aSai', methods=['GET', 'POST'])
def question10aSai():
    num1 = request.form.get("RangeStart")
    num2 = request.form.get("RangeEnd")  
    starttime=timer()
    query_str = "select id,place from dbo.all_month where nst>='{}' and nst<'{}'".format(num1,num2)
    crsr.execute(query_str)    
    list1 = crsr.fetchall()
    time = timer() - starttime
    return render_template('question10aSai.html', list1 = list1, time  = time)  

@app.route('/question10bSai', methods=['GET', 'POST'])
def question10bSai():
    n = request.form.get("N")
    net = request.form.get("Net")  
    off = str(random.randint(1,9))
    starttime=timer()
    query_str = "select top "+n+" * from (select * from dbo.all_month where net = '"+net+"' ORDER BY id OFFSET "+off+" ROWS) a"
    print(query_str)
    crsr.execute(query_str)    
    list1 = crsr.fetchall()
    time = timer() - starttime
    return render_template('question10bSai.html', list1 = list1, time  = time)  

@app.route('/question11Sai', methods=['GET', 'POST'])
def question11Sai():
    num1 = request.form.get("RangeStart")
    num2 = request.form.get("RangeEnd")  
    n = request.form.get("N")
    net = request.form.get("Net")  
    off = str(random.randint(1,9))
    t = int(request.form.get("T")) 
    timeList1 = []
    timeList2 = []
    sum = 0
    hashvalue1 = "Chetan11"
    redis_key1 = "{}".format(hashvalue1)
   
    for i in range(0,t):
        if not (r.get(redis_key1)):
            print("Caching from Database")
            starttime = timer()
            query_str1 = "select id,place from dbo.all_month where nst>='{}' and nst<'{}'".format(num1,num2)
            crsr.execute(query_str1)    
            data = crsr.fetchall()
            r.set(redis_key1, pickle.dumps(list(data)))
            time = timer() - starttime
            timeList1.append(time)
            sum = sum + time
        else:
            print("Caching from redis")
            starttime = timer()
            data = pickle.loads(r.get(redis_key1))
            time = timer() - starttime
            timeList1.append(time)
            sum = sum + time

    hashvalue2 = "Chetan32"
    redis_key2 = "{}".format(hashvalue2)
    
    for i in range(0,t):
        if not (r.get(redis_key2)):
            print("Caching from Database")
            starttime = timer()
            query_str2 = "select top "+n+" * from (select * from dbo.all_month where net = '"+net+"' ORDER BY id OFFSET "+off+" ROWS) a"
            crsr.execute(query_str2)    
            data = crsr.fetchall()
            r.set(redis_key2, pickle.dumps(list(data)))
            time = timer() - starttime
            timeList2.append(time)
            sum = sum + time
        else:
            print("Caching from redis")
            starttime = timer()
            data = pickle.loads(r.get(redis_key2))
            time = timer() - starttime
            timeList2.append(time)
            sum = sum + time

    return render_template('Question11Sai.html', list1 = timeList1, list2= timeList2, total = sum)  

@app.route('/question12Sai', methods=['GET', 'POST'])
def question12Sai():
    timeLista = []
    timeListb = []
    sum = 0
    t = int(request.form.get("T"))
    hashvalue1 = "Chetan11"
    redis_key1 = "{}".format(hashvalue1)
    
    for i in range(0,t):
        if r.get(redis_key1):
            print("Caching from redis")
            starttime = timer()
            data = pickle.loads(r.get(redis_key1))
            time = timer() - starttime
            timeLista.append(time)
            sum = sum + time
        
    hashvalue2 = "Chetan32"
    redis_key2 = "{}".format(hashvalue2)
    
    for i in range(0,t):
        if r.get(redis_key2):
            print("Caching from redis")
            starttime = timer()
            data = pickle.loads(r.get(redis_key2))
            time = timer() - starttime
            timeListb.append(time)
            sum = sum + time

    return render_template('Question12Sai.html', list1 = timeLista, list2= timeListb, total = sum)

   
if __name__ == "__main__":
    app.run(debug=True)