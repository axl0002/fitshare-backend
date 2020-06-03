from flask import Flask, request, Response
import psycopg2
import os
from flask import jsonify
from datetime import datetime
import s3

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

    return Response(status=201)


@application.route('/send', methods=["POST"])
def send():
    json = request.get_json()
    user_id = json["user_id"]
    targets_ids = json["targets_ids"]
    metadata = json["metadata"]
    print(json)

    # TODO: pass file and save it to storage

    cur = db_conn.cursor()
    cur.execute("SELECT friendsid FROM friends WHERE sourceid = %s AND targetid IN %s",
                (user_id, tuple(targets_ids))
                )
    friends_ids = cur.fetchall()
    print(friends_ids)
    for friends_id in friends_ids:
        dt = datetime.now()
        cur.execute("INSERT INTO friend_challenges (metadata, friendsid, is_complete)"
                    " VALUES (%s, %s, %s) RETURNING challengeid",
                    (metadata, friends_id[0], False))
        challenge_id = cur.fetchone()[0]
        print(challenge_id)
        db_conn.commit()
    cur.close()
    return Response(status=201)
