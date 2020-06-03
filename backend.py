from flask import Flask, request, Response
import psycopg2
import os
from flask import jsonify

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
    cur.execute("SELECT name FROM appuser WHERE userid IN (SELECT targetid FROM friends WHERE (sourceid = %s));", (userid,) )
    res = cur.fetchall()
    cur.close()
    return jsonify(res)


@application.route('/user', methods=["POST"])
def new_user():
    user_data = request.get_json()["user"]

    cur = db_conn.cursor()
    cur.execute("INSERT INTO appuser VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (user_data["id"], user_data["email"], user_data["name"]))
    db_conn.commit()
    cur.close()

    return Response(status=200)
