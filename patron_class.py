import psycopg


class Patrons():
    con = None
    
    def connect2db(self, host, port, database, user, password):
        strConnInfo = "conninfo=\'host={} port={} dbname={} user={} password={} connection_timeout=10\'"\
            .format(host, port, database, user, password)

        con = psycopg.connect(strConnInfo)
        return con



