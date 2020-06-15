from flask import Flask, request, Response
import psycopg2
import os
from flask import jsonify
import json
from datetime import datetime

import s3

application = Flask(__name__)
db_conn = psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


@application.route('/join', methods=["PUT"])
def join_channel():
    userid = request.get_json()["userid"]
    group_id = request.get_json()["groupid"]
    cur = db_conn.cursor()
    cur.execute("INSERT INTO subscriptions (userid, group_id) VALUES (%s, %s)", (userid, int(group_id)))
    db_conn.commit()
    cur.close()
    return Response(status=201)


@application.route('/leave', methods=["DELETE"])
def leave_channel():
    userid = request.get_json()["userid"]
    group_id = request.get_json()["groupid"]
    cur = db_conn.cursor()
    cur.execute("DELETE FROM subscriptions WHERE  userid = %s AND group_id = %s", (userid, int(group_id)))
    db_conn.commit()
    cur.close()
    return Response(status=204)


@application.route('/channels/<userid>')
def channels(userid):
    cur = db_conn.cursor()
    cur.execute("SELECT g.group_id, group_name, group_description, group_avatar_name, COUNT(userid) "
                "FROM groups AS g "
                "LEFT JOIN subscriptions AS s ON g.group_id = s.group_id "
                "WHERE g.group_id IN ( "
                "SELECT group_id "
                "   FROM subscriptions "
                "   WHERE userid = %s"
                ") "
                "GROUP BY g.group_id, group_name, group_description, group_avatar_name "
                "ORDER BY COUNT(userid) DESC", (userid,))
    user_channels = cur.fetchall()

    cur.execute("SELECT g.group_id, group_name, group_description, group_avatar_name, COUNT(userid) "
                "FROM groups AS g "
                "LEFT JOIN subscriptions AS s ON g.group_id = s.group_id "
                "WHERE g.group_id NOT IN ( "
                "SELECT group_id "
                "   FROM subscriptions "
                "   WHERE userid = %s"
                ") "
                "GROUP BY g.group_id, group_name, group_description, group_avatar_name "
                "ORDER BY COUNT(userid) DESC", (userid,))
    other = cur.fetchall()

    response = {"user_channels":
                [{"id": d[0], "name": d[1], "description": d[2], "avatar": d[3], "count": d[4]} for d in
                    user_channels],
                "other": [{"id": d[0], "name": d[1], "description": d[2], "avatar": d[3], "count": d[4]} for d in other]
                }
    cur.close()
    return jsonify(response)


@application.route('/friends/<userid>')
def get_friends(userid):
    cur = db_conn.cursor()
    cur.execute("SELECT targetid "
                "FROM friends "
                "WHERE sourceid = %s ", (userid,))
    friend_list = cur.fetchall()
    if friend_list is None or not friend_list:
        return jsonify([])

    cur.execute("SELECT DISTINCT userid, name "
                "FROM appuser "
                "LEFT JOIN friends "
                "   ON userid = targetid "
                "WHERE userid IN %s", (tuple(friend_list),))
    friends = cur.fetchall()
    friends_ids = tuple([friend[0] for friend in friends])

    cur.execute("WITH streaks AS ( "
                "   WITH RECURSIVE cte AS ("
                "       SELECT friendsid, time, 1 as cnt "
                "       FROM friend_challenges "
                "       WHERE EXTRACT(epoch from now()) - EXTRACT(epoch from time) < 86400 "
                "       UNION ALL "
                "       SELECT a.friendsid, a.time, c.cnt + 1 "
                "       FROM friend_challenges a "
                "       INNER JOIN cte c ON a.friendsid = c.friendsid AND a.time = c.time - interval '1' day "
                "   ) "
                "   SELECT friendsid, MAX(cnt) AS streak "
                "   FROM cte "
                "   GROUP BY friendsid "
                ") "
                "SELECT t1.friendsid, sourceid, targetid, is_complete, time, coalesce(streak, 0) as streak "
                "FROM friend_challenges AS t1 "
                "RIGHT JOIN friends ON friends.friendsid = t1.friendsid "
                "LEFT JOIN streaks ON streaks.friendsid =  t1.friendsid "
                "WHERE (sourceid = %s AND targetid IN %s "
                "OR sourceid IN %s AND targetid = %s ) "
                "AND t1.time = (SELECT MAX(t2.time) FROM friend_challenges AS t2 WHERE t2.friendsid = t1.friendsid);",
                (userid, friends_ids, friends_ids, userid))
    streaks = cur.fetchall()

    streaks_dict = dict()
    for id in friends_ids:
        streaks_dict[id] = dict()
        streaks_dict[id]["from"] = dict()
        streaks_dict[id]["to"] = dict()

    for item in streaks:
        if item[1] == userid:  # sourceid = userid
            key = item[2]
            where = "from"
        else:
            key = item[1]
            where = "to"
        streaks_dict[key][where]["is_complete"] = item[3]
        streaks_dict[key][where]["time"] = item[4]
        streaks_dict[key][where]["streak"] = item[5]

    response = []
    for fr in friends:
        result = dict()
        result["id"] = fr[0]
        result["name"] = fr[1]
        status, streak, time = process_status_and_streak(streaks_dict[fr[0]]["from"], streaks_dict[fr[0]]["to"])
        result["streak"] = streak
        result["status"] = status
        if time is None:
            result["time"] = time
        else:
            result["time"] = time.strftime("%d-%m-%y")
        response.append(result)

    cur.close()
    return jsonify(response)


def process_status_and_streak(from_dict, to_dict):
    if not from_dict and not to_dict:
        return "NEW FRIEND", 0, None
    elif not from_dict:
        if to_dict["is_complete"]:
            return "OPENED", to_dict["streak"], to_dict["time"]
        else:
            return "NEW", to_dict["streak"], to_dict["time"]
    elif not to_dict:
        if from_dict["is_complete"]:
            return "COMPLETE", from_dict["streak"], from_dict["time"]
        else:
            return "SENT", from_dict["streak"], from_dict["time"]
    else:
        if from_dict["time"] > to_dict["time"]:
            if from_dict["is_complete"]:
                return "COMPLETE", from_dict["streak"], from_dict["time"]
            else:
                return "SENT", from_dict["streak"], from_dict["time"]
        else:
            if to_dict["is_complete"]:
                return "OPENED", to_dict["streak"], to_dict["time"]
            else:
                return "NEW", to_dict["streak"], to_dict["time"]


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
    with open(filename, 'rb') as video:
        s3.upload_file_obj(video, filename)

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
    cur.execute(
        "SELECT metadata, s3_key, challengeid FROM friend_challenges WHERE friendsid = %s ORDER BY time DESC LIMIT 1",
        (friend_id,))
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
    db_conn.commit()
    cur.close()
    return jsonify({
        "metadata": metadata,
        "key": key}
    )


@application.route('/data/<userid>', methods=["GET"])
def get_profile_data(userid):
    cur = db_conn.cursor()
    total_challenges_sent_sql = """
    select
        Count(*)
    from
        friend_challenges
    where
        friendsid in 
        (select
            friendsid
        from
            friends
        where
            sourceid=%s);
    """

    total_challenges_done_sql = """
    select
        count(*)
    from
        friend_challenges
    where
        is_complete=TRUE
        and
        friendsid in
            (select
                friendsid
            from
                friends
            where
                targetid=%s);
    """

    activity_by_day = """
    select
        friend_challenges.time::date,
        count(*)
    from
        friend_challenges
    where
        friendsid in
        (select
            friendsid
        from
            friends
        where
            sourceid=%s)
    group by 1
    order by 1;
    """

    cur.execute(total_challenges_sent_sql, (userid,))
    challenges_sent = cur.fetchone()

    cur.execute(total_challenges_done_sql, (userid,))
    challenges_done = cur.fetchone()

    if (challenges_sent[0]>0):
        cur.execute(activity_by_day, (userid,))
        challenges_by_day = {((str(val[0]))[-4:]): val[1] for val in cur.fetchall()}
    else:
        challenges_by_day = {}

    cur.close()
    resp = {
        "challengesSent": challenges_sent[0],
        "challengesDone": challenges_done[0],
        "challengesByDay": challenges_by_day,
    }
    return jsonify(resp)
