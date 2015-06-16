import MySQLdb as mdb
import datetime

USER = 'ubuntu'
HOST = '52.8.205.143'
DATABASE = 'insight'
PW = 'geheim'  # please don't steal the secret password


def get_connection():
    """ establish connection to remote DB """
    con = mdb.connect(
        user=USER,
        passwd=PW,
        host=HOST,
        db=DATABASE)
    return con


def get_cursor(con=None):
    """ get a cursur for dq query and insertion """
    if not con:
        con = get_connection()
    cur = con.cursor(mdb.cursors.DictCursor)
    return cur


def _to_timestamp(dt):
    """ convert date time to MySQL timestamp string """
    assert isinstance(dt, datetime.datetime)
    ts = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    return ts


def store_prod_msg(msg_id, topic, producer, 
                   produced_at, exp_started_at):
    """ strore message by producer in DB"""
    con = get_connection()
    cur = get_cursor(con)
    s = ("INSERT INTO ProducedMsg "
         "(msg_id, topic, producer, produced_at, exp_started_at) "
         "VALUES (%d, '%s', '%s', '%s', '%s'); " 
         % (msg_id, topic, producer, 
         _to_timestamp(produced_at), _to_timestamp(exp_started_at)))
    print s
    con.commit()


