import psycopg

def connect2db(host, port, database, user, password):
    strConnInfo = "conninfo=\'host={} port={} dbname={} user={} password={} connection_timeout=10\'"\
        .format(host, port, database, user, password)

    con = psycopg.connect(strConnInfo)
    return con

