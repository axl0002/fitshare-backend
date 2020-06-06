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

    cur.execute("WITH RECURSIVE cte "
                "AS ( "
                "SELECT friendsid, time, 1 as cnt "
                "FROM friend_challenges "
                "WHERE extract(epoch from now()) - extract(epoch from time) < 86400 "
                "UNION ALL "
                "SELECT a.friendsid, a.time, c.cnt + 1 "
                "FROM friend_challenges a "
                "INNER JOIN cte c ON a.friendsid = c.friendsid AND a.time = c.time - interval '1' day "
                ") "
                "SELECT friendsid, MAX(cnt) AS most_consecutive "
                "FROM cte "
                "GROUP BY friendsid")
    streaks = cur.fetchall()
    streaks_dict = dict()
    for streak in streaks:
        streaks_dict[streak[0]] = streak[1]

    cur.execute("SELECT userid, name FROM appuser WHERE userid IN (SELECT targetid FROM friends WHERE (sourceid = %s))", (userid,) )
    friends = cur.fetchall()
    response = []
    for fr in friends:
        result = dict()
        result["id"] = fr[0]
        result["name"] = fr[1]
        cur.execute("SELECT is_complete, time, friend_challenges.friendsid FROM friend_challenges "
                    "LEFT JOIN friends on friends.friendsid = friend_challenges.friendsid "
                    "WHERE sourceid = %s AND targetid = %s "
                    "ORDER BY time DESC "
                    "LIMIT 1", (userid, fr[0]))
        last_snap_from = cur.fetchone()
        cur.execute("SELECT is_complete, time, friend_challenges.friendsid FROM friend_challenges "
                    "LEFT JOIN friends on friends.friendsid = friend_challenges.friendsid "
                    "WHERE sourceid = %s AND targetid = %s "
                    "ORDER BY time DESC "
                    "LIMIT 1", (fr[0], userid))
        last_snap_to = cur.fetchone()
        if last_snap_from is None and last_snap_to is None:
            result["status"] = "NEW FRIEND"
            result["streak_to"] = 0
            result["streak_from"] = 0
        elif last_snap_from is None:
            result["streak_from"] = 0
            if last_snap_to[2] in streaks_dict.keys():
                result["streak_to"] = streaks_dict[last_snap_to[2]]
            else:
                result["streak_to"] = 0
            if last_snap_to[0]:  # if is_complete
                result["status"] = "OPENED"
            else:
                result["status"] = "SENT"
        elif last_snap_to is None:
            result["streak_to"] = 0
            if last_snap_from[2] in streaks_dict.keys():
                result["streak_from"] = streaks_dict[last_snap_from[2]]
            else:
                result["streak_from"] = 0
            if last_snap_from[0]:  # if is_complete
                result["status"] = "COMPLETE"
            else:
                result["status"] = "NEW"
        else:
            if last_snap_from[2] in streaks_dict.keys():
                result["streak_from"] = streaks_dict[last_snap_from[2]]
            else:
                result["streak_from"] = 0
            if last_snap_to[2] in streaks_dict.keys():
                result["streak_to"] = streaks_dict[last_snap_to[2]]
            else:
                result["streak_to"] = 0
            if last_snap_from[1] > last_snap_to[1]:
                if last_snap_from[0]:  # if is_complete
                    result["status"] = "COMPLETE"
                else:
                    result["status"] = "NEW"
            else:
                if last_snap_to[0]:  # if is_complete
                    result["status"] = "OPENED"
                else:
                    result["status"] = "SENT"
        response.append(result)
    cur.close()
    return jsonify(response)


def create_dict(friends):
    lists = []
    length = len(friends)
    for i in range(length):
        result = {
            "id": friends[i][0],
            "name": friends[i][1],
        }
        lists.append(result)
    return lists


@application.route('/add', methods=["POST"])
def add_friends():
    sourceid = request.get_json()["source_id"]
    targetemail = request.get_json()["target_email"]

    cur = db_conn.cursor()
    print(sourceid)
    print(targetemail)
    cur.execute("SELECT COUNT(*) FROM appuser WHERE (email = %s)", (targetemail,))
    count = [item[0] for item in cur.fetchall()]

    if (count[0] != 1):
        cur.close()
        return Response(status=400)
    else:
        cur.execute("SELECT userid FROM appuser WHERE (email = %s)", (targetemail,))
        target = [item[0] for item in cur.fetchall()]
        print(target[0])
        targetid = target[0]
        cur.execute("SELECT COUNT(*) FROM friends WHERE (sourceid = %s and targetid = %s)", (sourceid, targetid))
        cnt = [item[0] for item in cur.fetchall()]
        print(cnt)
        if (cnt[0] != 0):
            cur.close()
            return Response(status=400)
        else:
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
    if table_key is None:
        key = 0
    else:
        key = table_key[0] + 1

    file = request.files["file"]
    # TODO: avoid that?)
    filename = '{}.mp4'.format(key)
    file.save(filename)
    print(filename)
    s3.upload_file(filename, str(key))
    os.remove(filename)

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

# Maybe we shouldn't expose s3_key like that, suffices for now
@application.route('/open/<userid>/<friendid>', methods=["GET"])
def open_challenge(userid, friendid):
    cur = db_conn.cursor()
    cur.execute("SELECT friendsid FROM friends WHERE sourceid = %s AND targetid = %s",
                (userid, friendid))
    friend_id = cur.fetchone()[0]
    cur.execute("SELECT metadata, s3_key, challengeid FROM friend_challenges WHERE friendsid = %s ORDER BY time DESC LIMIT 1", (friend_id,))
    # TODO: None checks!
    res = cur.fetchone()
    metadata = res[0]
    key = res[1]
    challenge_id1 = res[2]

    cur.execute("SELECT friendsid FROM friends WHERE sourceid = %s AND targetid = %s",
                (friendid, userid))
    friend_id2 = cur.fetchone()[0]
    cur.execute(
        "SELECT challengeid FROM friend_challenges WHERE friendsid = %s ORDER BY time DESC LIMIT 1",
        (friend_id2,))
    challenge_id2 = cur.fetchone()[0]

    cur.execute("UPDATE friend_challenges SET is_complete = true WHERE challengeid IN (%s, %s)",
                (challenge_id1, challenge_id2))

    cur.close()
    return jsonify({
        "metadata": metadata,
        "key": key}
    )
