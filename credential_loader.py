#!/opt/rh/rh-python34/root/usr/bin/python3
import sys
import psycopg2
#from psycopg import errorcodes
import csv
import datetime
import re
import smtplib

DEBUG = False

def connect2db():
    con = psycopg2.connect(database='Patrons', 
                           host='librariesdev.mercer.edu', 
                           port=5432, 
                           user='patron_read', 
                           password='CongratsGraduation2023!!')
    return con



def housekeeping():
    con = connect2db()

    ### Deleting mobile credentials that have not been in the system for over 2 weeks  ###

    cleanQuery = """DELETE FROM mobile_credentials WHERE updated < now()-interval '2 weeks'
                 """
    c = con.cursor()
    try:
        c.execute(cleanQuery,)
        con.commit()
    except Exception as e:
        print ('Housekeeping error exception: ', e)
    c.close()


if __name__ == '__main__':
    stats = {'duplicates':0,'new':0,'updates':0,'enrollments':0}

    con = connect2db()
    try:
        file = sys.argv[1]
    except:
        print("Usage: 'credential_loader.py filename' where filename is a comma-delimited file containing\npatron tap records in the predefined format.\n")
        sys.exit()

    fileHandle = open(file,"r",encoding='utf8')

    insertQuery = """INSERT INTO mobile_credentials
        (cardnum, name, muid, cardtype, status)
        VALUES (
            %s, %s, %s, %s, %s);"""

    updateQuery = """UPDATE mobile_credentials
                                     SET
                                        name = %s, muid = %s, cardtype = %s, status = %s, updated = now()
                                     WHERE
                                        cardnum = %s
                                """



    count = 0
    updateCount = 0
    badmuid = []

    sender = 'pham_cs@mercer.edu'
    to = ['pham_cs@mercer.edu']

    message =  "To: %s\r\n" % sender
    message += "MIME-Version: 1.0\r\n"
    message += "Content-type: text/html\r\n"
    message += "Subject: Mobile Credential Load Report\r\n"
    message += "Error patrons:<br/> \r\n"
    noerrmessage = True


    for line in fileHandle:
        cred = {}
        try:
            newline = line.replace('"','')
            m = re.search(r'([0-9]{22}),(.*),([0-9]{22}),([a-z]+),([a-z]+)',newline, re.IGNORECASE)
            cred['muid'] = m.group(1).lstrip('0')
            cred['name'] = m.group(2)
            cred['cardnum'] = m.group(3)
            cred['cardtype'] = m.group(4)
            cred['status'] = m.group(5)
                



            count =+ 1
            c = con.cursor()

            try:
                data = (
                    cred['cardnum'],
                    cred['name'],
                    cred['muid'],
                    cred['cardtype'],
                    cred['status'],
                    )
                c.execute(insertQuery,data)
                con.commit()
            except:
                con.rollback()
                data = (
                    cred['name'],
                    cred['muid'],
                    cred['cardtype'],
                    cred['status'],
                    cred['cardnum'],
                    )
                try:
                    c.execute(updateQuery,data)
                    con.commit()
                    updateCount += 1
                except Exception as e:
                    print ('Error with ',data, ' exception: ',e)
                    message += line
                    message += "<br/>\r\n"
                    noerrmessage = False


        except: 
            m= None


    housekeeping()

    if updateCount < 10:
        noerrmessage = False
        message += "Error with the update <br/>\r\n"
#        sender = 'pham_cs@mercer.edu'
#        to = ['pham_cs@mercer.edu']

#        message =  "To: %s\r\n" % sender
#        message += "MIME-Version: 1.0\r\n"
#        message += "Content-type: text/html\r\n"
#        message += "Subject: Mobile Credential Load Report\r\n"
#        message += "%s duplicate staff handled<br/>\r\n" % (stats['duplicates'],)
#        message += "%s new staff information handled<br/>\r\n" % (stats['enrollments'],)
#        message += "%s new patrons<br/>\r\n" % (stats['new'],)
#        message += "%s updated patrons<br/>\r\n" % (stats['updates'],)
#        message += "Expired %s patrons<br/>\r\n" % (the_count,)
#    for i in badmuid:
#        message += str(badmuid[i])+"\n"
    if DEBUG:
        print( message)
    if not (noerrmessage):
        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, to, message)
            if DEBUG:
                print( "Successfully sent email")
        except smtplib.SMTPException as e:
            print( "Error: unable to send email %s" % str(e))
            print( "message:\r\n%s" % message)
