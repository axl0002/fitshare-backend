from flask import Flask, request, Response
import psycopg2
import os
from flask import jsonify
import json
import s3

application = Flask(__name__)
db_conn = psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


@application.route('/friends/<userid>')
def get_friends(userid):
    cur = db_conn.cursor()
    # posible bug if queries dont return same order
    cur.execute("SELECT (name) FROM appuser WHERE userid IN (SELECT targetid FROM friends WHERE (sourceid = %s))", (userid,) )
    names = [item[0] for item in cur.fetchall()]
    cur.execute("SELECT (userid) FROM appuser WHERE userid IN (SELECT targetid FROM friends WHERE (sourceid = %s))", (userid,) )
    ids = [item[0] for item in cur.fetchall()]

    cur.close()
    return jsonify(create_dict(names, ids))

def create_dict(names, ids):
    lists = []
    length = len(names)
    for i in range(length):
        dict = {
        "id": ids[i],
        "name": names[i],
        }
        lists.append(dict)
    return lists


@application.route('/add', methods=["POST"])
def add_friends():
    sourceid = request.get_json()["source_id"]
    targetemail = request.get_json()["target_email"]

    cur = db_conn.cursor()
    print(sourceid)
    print(targetemail)
    cur.execute("SELECT count(*) FROM appuser WHERE (email = %s)", (targetemail,))
    count = [item[0] for item in cur.fetchall()]

    if (count[0] != 1):
        return Response(status=400)
    else:
        cur.execute("SELECT userid FROM appuser WHERE (email = %s)", (targetemail,))
        targetid = [item[0] for item in cur.fetchall()]
        print(targetid[0])
        cur.execute("INSERT INTO friends (sourceid, targetid) VALUES (%s, %s)", (sourceid, targetid))
        cur.execute("INSERT INTO friends (sourceid, targetid) VALUES (%s, %s)", (targetid, sourceid))

        db_conn.commit()
        cur.close()

        return Response(status=201)


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
    cur = db_conn.cursor()
    cur.execute("SELECT s3_key FROM friend_challenges ORDER BY s3_key DESC LIMIT 1")
    table_key = cur.fetchone()
    print(table_key)
    if table_key is None:
        key = 0
    else:
        key = table_key[0] + 1
    print(key)

    file = request.files["file"]
    s3.upload_file_obj(file, str(key))

    d = json.loads(request.form['json'])
    user_id = d["user_id"]
    targets_ids = d["targets_ids"]
    metadata = d["metadata"]

    cur.execute("SELECT friendsid FROM friends WHERE sourceid = %s AND targetid IN %s",
                (user_id, tuple(targets_ids))
                )
    friends_ids = cur.fetchall()
    for friends_id in friends_ids:
        cur.execute("INSERT INTO friend_challenges (metadata, friendsid, is_complete, s3_key)"
                    " VALUES (%s, %s, %s, %s)",
                    (metadata, friends_id[0], False, key))
        db_conn.commit()
    cur.close()
    return Response(status=201)
