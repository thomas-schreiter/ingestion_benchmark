import MySQLdb as mdb
import sys

USER = 'ubuntu'
HOST = '52.8.205.143'
DATABASE = 'insight'
PW = 'geheim'


def main():
    try:
        # establish connection to remote DB
        con = mdb.connect(
            user=USER,
            passwd=PW,
            host=HOST,
            db=DATABASE)
        print con
        # query
        cur = con.cursor()
        cur.execute("SELECT VERSION()")
        ver = cur.fetchone()

        print "Database version: %s " % ver

    except mdb.Error as e:
        print "Error %d: %s % (e.args[0], e.args[1])"
        sys.exit(1)

    finally:
        if con:
            con.close()
        

if __name__ == '__main__':
    main()


