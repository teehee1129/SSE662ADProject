# -*- coding: utf-8 -*-
import unittest
from unittest import TestCase
import psycopg
from psycopg import errors

class TestDBConn(unittest.TestCase):
    
   con = None
   host = 'librariesdev.mercer.edu'
   port = 5432
   database = 'Patrons'
   user = 'patrons_read'
   password = 'CongratsGraduation2023!!'
   
   
   def connect2db(self, host, port, database, user, password):
       
       strConnInfo = "conninfo=\'host={} port={} dbname={} user={} password={} connection_timeout=10\'"\
           .format(host, port, database, user, password)

       con = psycopg.connect(strConnInfo)
   
       cursor = con.cursor()
       cursor.execute("SELECT name, muid FROM students WHERE name = 'Alisha Juman';")
       result = cursor.fetchall()
       expected = [('Alisha Juman', 10995721)]
       if result:
           return True
       else:
           return False
       
       self.assertEqual(result, expected)
       self.assertTrue(result == expected)
       con.close()  
   
    
if __name__ == '__main__':
    unittest.main()
    
    
