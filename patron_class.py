# -*- coding: utf-8 -*-
"""
Created on Fri May  5 20:02:29 2023

@author: Alisha
"""

import psycopg


class Patrons():
    con = None
    
    def connect2db(self, host, port, database, user, password):
        strConnInfo = "conninfo=\'host={} port={} dbname={} user={} password={} connection_timeout=10\'"\
            .format(host, port, database, user, password)

        con = psycopg.connect(strConnInfo)
        return con
    
    state = None
    
    def __init__(self, state):
        self.setProgram(state)
        
    def setProgram(self, state):
        self.state = state
        self.state.program = self
        
    def presentState(self):
        print("Program is in " + (self.state))
        
    def importCSV(self):
        self.state.importCSV()
        
    def addToDB(self):
        self.state.addToDB()
        
    def retrieveFromDB(self):
        self.state.retrieveFromDB()
        
    def exportXML(self):
        self.state.exportXML()
  
        
class progressStatus():
    
    def importCSV(self) -> None:
        print("Data is getting imported from CSV file")
        
    def addToDB(self) -> None:
        print("Data is being added to database")
        
    def retrieveFromDB(self) -> None:
        print("Data is be retrieved from database")
        
    def exportXML(self) -> None:
        print("Data is getting exported into XML file")
    
    
if __name__ == "__main__":
    # The client code.

    myProgramState = Patrons(progressStatus())
    myProgramState.presentState()

        