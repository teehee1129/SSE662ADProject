# -*- coding: utf-8 -*-
import unittest
from unittest import TestCase, TestSuite
import psycopg
from student_sis_loader import students
from employee_sis_loader import employees
from xmltest import XMLAssertions

class TestXMLtransfer(unittest.TestCase, XMLAssertions):
   
    results = None
   
    for student in students:
        results.append(student.get_json())

    for employee in employees:
        results.append(employee.get_json())

   
    def assert_XPath_query(self):
        response_xml = 'XML file student_sis_loader creates'
    
        self.assertXPathNodeText(response_xml, 'Juman', 'last')
        self.assertXPathNodeText(response_xml, 'Alisha', 'first')
        self.test.assertEqual.assert_called_once_with('Juman', 'Juman')
        self.test.assertEqual.assert_called_once_with('Alisha', 'Alisha')
   

if __name__ == '__main__':
    unittest.main()
    
    
