# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:07:04 2023

@author: Alisha
"""

import json
import xml.etree.ElementTree as et

class Patron:
    def __init__(self, muid, email, classStatus):
        self.muid = muid
        self.email = email
        self.classStatus = classStatus


class XMLBuilder:
    def builder(self, patron, format):
        if format == 'JSON':
            patron_info = {
                'muid': patron.muid,
                'email': patron.email,
                'class status': patron.classStatus
            }
            return json.dumps(patron_info)
        elif format == 'XML':
            patron_info = et.Element('patron', attrib={'muid': patron.muid})
            email = et.SubElement(patron_info, 'email')
            email.text = patron.email
            classStatus = et.SubElement(patron_info, 'class_status')
            classStatus.text = patron.classStatus
            return et.tostring(patron_info, encoding='unicode')
        else:
            raise ValueError(format)