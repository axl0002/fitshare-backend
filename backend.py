from flask import Flask, request
import psycopg2
import os
import json
# from flask import jsonify

application = Flask(__name__)
db_conn = psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


@application.route('/users')
def get_all_users():
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM USERS;")
    res = cur.fetchall()
    cur.close()
    return str(res)

@application.route('/friends/<userid>')
def get_friends(userid):
    cur = db_conn.cursor()
    cur.execute("SELECT targetid FROM friends WHERE (sourceid =" + userid + ");")
    res = cur.fetchall()
    cur.close()
    return json.dumps(res)
    
@application.route('/user', methods=["POST"])
def new_user():
    print(request.get_json())
