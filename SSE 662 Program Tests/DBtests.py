# -*- coding: utf-8 -*-
import unittest
from unittest import TestCase, TestSuite
import psycopg
from psycopg import errors
from lxml import etree
import os
from xmltest import XMLAssertions

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

class TestDBConn(unittest.TestCase):
    
    def test_select_name_muid(self):
        conn = psycopg.connect(dbname='Patrons',
                               host='librariesdev.mercer.edu',
                               port=5432,
                               user='patron_read',
                               password='CongratsGraduation2023!!')
        cursor = conn.cursor()
        cursor.execute("SELECT name, muid FROM students WHERE name = 'Alisha Juman';")
        result = cursor.fetchall()
        expected = [('Alisha Juman', 10995721)]
        self.assertEqual(result, expected)
        conn.close()  
        
class TestCSVtrasnfer(unittest.TestCase):
    
    def completeTest(self):
        pathCSV = r'pathToCSV\testCSV.csv'
 
        with open(pathCSV, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                for item in row:
                    try:
                        getattr(student_loader.Students, item)()
                    except AttributeError:
                        print("Unknown attribute", item, "ignored")
 
    @staticmethod
    def myTests():
        suite = unittest.TestSuite()
        suite.addTest(TestSuite('completeTest'))
        return suite
    

class TestXMLtransfer(unittest.TestCase, XMLAssertions):
    
    def assert_XPath_query(self):
        response_xml = 'XML file student_sis_loader creates'
    
        self.assertXPathNodeText(response_xml, 'Juman', 'last')
        self.assertXPathNodeText(response_xml, 'Alisha', 'first')
        self.test.assertEqual.assert_called_once_with('Juman', 'Juman')
        self.test.assertEqual.assert_called_once_with('Alisha', 'Alisha')
    
if __name__ == '__main__':
    unittest.main()
    
    
