import MySQLdb as mdb
import sys

USER = 'ubuntu'
HOST = '52.8.205.143'
DATABASE = 'insight'
PW = 'geheim'


def main():
    # establish connection to remote DB
    con = mdb.connect(
        user=USER,
        passwd=PW,
        host=HOST,
        db=DATABASE)
    print con
   
    with con:
        # query
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM hello")
        rows = cur.fetchall()
        for row in rows:
            print row


if __name__ == '__main__':
    main()


