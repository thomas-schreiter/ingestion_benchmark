import MySQLdb as mdb
import datetime
import prettytable

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
    assert dt is not None
    assert isinstance(dt, datetime.datetime)
    ts = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    return ts


def _insert(insertstr):
    """ execute string on db, execstr must be INSERT statement"""
    assert "insert" in insertstr.lower()
    con = get_connection()
    cur = get_cursor(con)
    cur.execute(insertstr)
    print insertstr
    con.commit()


def store_prod_msg(seq, topic, producer, 
                   produced_at, throughput, exp_started_at):
    """ store message by producer in DB"""
    if throughput is None:
        throughput = 'NULL'
    s = ("INSERT INTO ProducedMsg "
         "(seq, topic, producer, throughput_msg_per_sec, produced_at, exp_started_at) "
         "VALUES (%d, '%s', '%s', %s, '%s', '%s'); " 
         % (seq, topic, producer, throughput, 
         _to_timestamp(produced_at), _to_timestamp(exp_started_at)))
    _insert(s)


def store_con_msg(seq, topic, consumer, broker, 
                  consumed_at, throughput, delay, exp_started_at):
    """ store message by consumer in DB"""
    if throughput is None:
        throughput = 'NULL'
    s = ("INSERT INTO ConsumedMsg "
         "(seq, topic, consumer, broker, throughput_msg_per_sec, delay_in_sec, " 
         "consumed_at, exp_started_at) "
         "VALUES (%d, '%s', '%s', '%s', %s, %f, '%s', '%s'); " 
         % (seq, topic, consumer, broker, throughput, delay.total_seconds(), 
         _to_timestamp(consumed_at), _to_timestamp(exp_started_at)))
    _insert(s)


def query_latest_throughput(topic):
    """ get the latest throughput for a specified topic """
    querystr = """
        SELECT SUM(throughput_msg_per_sec) AS throughput 
            FROM ProducedMsg p1 
            JOIN 
                
                (SELECT DISTINCT producer, max(seq) AS max_seq, p2.exp_started_at AS exp FROM ProducedMsg p2 
                    WHERE p2.exp_started_at = 
                        ( SELECT max(p3.exp_started_at) FROM ProducedMsg p3 
                            WHERE topic = '%s' ) 
                    GROUP BY producer LIMIT 20) X   
                                                
             ON  p1.producer = X.producer 
                 AND p1.seq = max_seq 
                 AND p1.exp_started_at = exp; """ % topic

    rows = query_rows(querystr)
    assert len(rows) == 1, "No rows found for query: %s" % querystr
    return rows[0]["throughput"]


def query_rows(querystr):
    con = get_connection()
    cur = get_cursor(con)
    cur.execute(querystr)
    rows = cur.fetchall()
    return rows


def _query_pretty(querystr):
    con = get_connection()
    cur = get_cursor(con)
    cur.execute(querystr)
    rows = cur.fetchall()
    header = False
    for row in rows:
        if not header:
            x = prettytable.PrettyTable(list(row.keys()))
            x.padding_width = 1
            header = True
        x.add_row(list(row.values()))
    con.close()
    return str(x)

