#!/bin/python
#/usr/bin/python2.6

import psycopg, datetime, sys, json
import connect2db

###################################
##     MARC Record Load Profile
###################################
# 080||a|0|8|p| |43|n|N|0|exp date
# 081||a|0|1|p| |44|n|N|0|pcode1
# 082||a|0|1|p| |45|n|N|0|pcode2
# 083||a|0|3|p| |46|n|N|0|pcode3
# 084||a|0|3|p| |47|n|N|0|ptype
# 085||a|0|5|p| |53|n|N|0|home libr
# 010||+|0|0|p|u|0|n|N|0|????
# 020||+|0|0|p|v|0|n|N|0|muid
# 030||+|0|0|p|b|0|n|N|0|barcode
# 035||+|0|0|p|g|0|n|N|0|cwid
# 100||+|0|0|p|n|0|n|N|0|name
# 220||+|0|0|p|a|0|n|N|0|address 1
# 225||+|0|0|p|t|0|n|N|0|phone 1
# 230||+|0|0|p|h|0|n|N|0|address 2
# 235||+|0|0|p|p|0|n|N|0|phone 2
# 550||+|0|0|p|z|0|n|N|0|email
# 555||+|0|0|p|d|0|n|N|0|dept

###################################
##    Input File Column Spec
###################################
#  0 muid
#  1 name
#  2 email
#  3 expdate
#  4 location_description
#  5 description
#  6 position_family
#  7 campus
#  8 division
#  9 operational_title

class Employees():
    con = None
    patron_dict = {}
    emeriti_tuple = ()
    def __init__(self,options):
        '''
            Instantiate our Employees object with access to command line options and
            arguments, as well as opening the database connection, and begin by 
            getting all the patrons.
        '''
        self.config = options

        dbhostname = self.config['patrons']['host']
        dbportnumber = int(self.config['patrons']['port'])
        dbname = self.config['patrons']['database']
        dbusername = self.config['patrons']['user']
        dbpassword = self.config['patrons']['password']

        self.con = connect2db(dbhostname, dbportnumber, dbname, dbusername, dbpassword)

        self.getAllPatrons()


    def getCursor(self):
        '''
            Just return a cursor from our current connection
        '''
        return self.con.cursor(row_factory=psycopg.rows.dict_row)

    def _formatDate(self,raw_date):
        '''
            Formatted according to III standards: datetime.strftime("%m%d%Y")
            No longer adhere to #313, because of #625
        '''
        #sys.stderr.write('date:{0}'.format(raw_date))
        return raw_date.strftime('%Y-%m-%d')

    def _setPC2(self,location_description,division,operational_title):

        '''
            Sets the School Statistical Category for the employee. If new division abbreviation is set, match it with
            an existing statistical category in Alma or create a new one if there is not a match. Then add that to this
            data dictionary.
        '''

        if not location_description==None:
            location_description=location_description.strip()
        if not division==None:
            division=division.strip()
        if not operational_title==None:
            operational_title=operational_title.strip()
        pc2Data_dict = {"LAW":"lawschool","MUSM":"medschool","MED":"medschool","CPS":"ccps","CPV":"ccps","PCM":"ccps","CLA":"cla","BUS":"stetson","EDU":"tift","NUR":"nursing","COP":"cop","PHS":"cop","CHP":"cohp","MERC":"merc","EGR":"engineering","MUS":"music","GUN":"general","THE":"theology","RAC":"general"}
        pcode2="error"
        if location_description=="Tarver Library" or location_description=="Swilley Library" or location_description=="Center Library Services":
            pcode2="library"
        elif division:
            if division in pc2Data_dict:
                pcode2=pc2Data_dict[division]
            else:
                pcode2="blankschool"
        elif operational_title:
            if operational_title.lower()=="adjunct":
                pcode2="adjunctschool"
        return pcode2
    
    def _setPC3(self,position_family,operational_title):

        '''
            Sets the employee Non-Degree Position Statistical Category for the employee. If new position_family abbreviation is set, match it with
            an existing statistical category in Alma or create a new one if there is not a match. Then add that to this
            data dictionary.
        '''

        if not position_family==None:
            position_family=position_family.strip()
        if not operational_title==None:
            operational_title=operational_title.strip()
        pc3Data_dict = {"ADJ":"adjunctadj","ADM":"adminadm","ADMR":"adminadmr","CLAS":"classified","FAC":"faculty","LIB":"librarylib","LIBR":"librarylibr","MERC":"dmerc","PROF":"prof","PROFR":"profr"}
        pcode3="facstaffadmin"
        if position_family:
            if position_family in pc3Data_dict:
                pcode3=pc3Data_dict[position_family]
            else:
                pcode3="blankposition"
        elif operational_title:
            if operational_title.lower()=="adjunct":
                pcode3="adjunctadj"
        return pcode3
    
    def _setHomeLib(self,campus,operational_title, PC2):

        '''
            Sets the home library Statistical Category for the employee. If new division abbreviation is set, match it with
            an existing statistical category in Alma or create a new one if there is not a match. Then add that to this
            data dictionary.
        '''

        if not campus==None:
            campus=campus.strip()
        if not operational_title==None:
            operational_title=operational_title.strip()
        hlibData_dict = {"ATL":"swil","COL":"colm","DVL":"doug","EMN":"tarv","HRY":"hen","LAW":"law","MCN":"tarv","MED":"musm","MRC":"tarv","NCR":"swil","NUR":"swil","NWN":"swil","OTH":"swil","PHS":"swil","PLN":"musm","RAC":"hen","SAV":"sav","THE":"swil"}
        hlib="zzzzz"
        if campus:
            if campus in hlibData_dict:
                hlib=hlibData_dict[campus]
        elif operational_title:
            if operational_title.lower()=="adjunct":
                hlib="hadjuct"
            else:
                hlib="tarv" 
        if PC2=="medschool":
            if hlib == "tarv":
                hlib = "musm"
            elif hlib == "swil":
                hlib = "atlmed"
            elif hlib != "colm" and hlib != "sav":
                hlib = "musm"
        if PC2=="lawschool":
            hlib = "law"


        return hlib

    def add_years(self, d, years):
        """Return a date that's `years` years after the date (or datetime)
        object `d`. Return the same calendar date (month and day) in the
        destination year, if it exists, otherwise use the following day
        (thus changing February 29 to March 1).

        """
        try:
            return d.replace(year = d.year + years)
        except ValueError:
            return d + (datetime.date(d.year + years, 1, 1) - datetime.date(d.year, 1, 1))

    def _setUserGroup(self, position_family, operational_title, PC2, PC3, camp, location_code):
        userGroup = "uempl"
        if PC2 == "lawschool":
            if position_family == "LIB" or position_family =="LIBR":
                userGroup = "lstaff"
            elif position_family =="CLAS":
                userGroup = "lempl"
            elif position_family =="FAC" or position_family =="PROF" or position_family=="ADM" or position_family=="ADMR":
                userGroup = "lfac"
            else:
                userGroup = "999"
            if operational_title.lower() =="adjunct":
                userGroup = "ladj"
        elif PC2 == "medschool":
            if PC3 == "colm" or camp=="COL":
                userGroup = "mcempl"
            elif PC3 =="sav" or camp=="SAV":
                userGroup = "msempl"
            else:
                userGroup = "mmempl"
        else:
            
            if position_family == "LIB" or position_family =="LIBR" or location_code=="Center Library Services" or location_code=="Swilley Library" or location_code=="Tarver Library":
                userGroup = "ulibstaff" 
        return userGroup


    def _setUserID (self, login, email):
        prime_id = ""
        if login !=None:
            prime_id = login
        else:
            prime_id = email.split('@')[0]
        return prime_id

    def patronRoleFill (self, librarystr, expir_date):
        role_json = {"@segment_type":"External",
                     "status":"ACTIVE",
                     "scope": librarystr,
                     "role_type":"200",
                     "expiry_date":expir_date
                    }
        return role_json

    
    def get_json(self,muid):
        '''
        Convert row of patron data into JSON as specified by the Sierra
        REST API
        '''
        data_dict = self.patron_dict[muid]

        operational = data_dict['operational_title']
        campus = data_dict['campus']
        location = data_dict['location_description']
        division = data_dict['division']
        muid = data_dict['muid']
        patron_muid = str(muid)
        email = data_dict['email']
        position = data_dict['position_family']
        login = data_dict['login']
        preferred_lang="en"
        user_id = self._setUserID(login, email)
        pc2_str = self._setPC2(location, division, operational)
        pc3_str = self._setPC3(position, operational)
        barcode3_json = '0'
        if position == "LIB" or position=="LIBR":
            record_type = "STAFF"
        else:
            record_type = "PUBLIC"
        status = "ACTIVE"




        usergroup_str = self._setUserGroup(position, operational, pc2_str, pc3_str, campus, location)
        campus_str = ""
        homelib_str = self._setHomeLib(campus, operational, pc2_str)
        stat_json = {}
        cat2str = ""

        nameSplit1 = data_dict['name'].split(',')
        name_len = len(nameSplit1)
        if name_len > 1:
            nameSplit2 = nameSplit1[1].lstrip()
            lastName = nameSplit1[0]
        else:
            lastName = nameSplit1[0]
            firstName = ' '
        if len(nameSplit2.split(' ')) > 1:
            firstName = nameSplit2.split(' ')[0]
            middleName = nameSplit2.split(' ')[1]
        else:
            firstName = nameSplit2.split(' ')[0]
            middleName = ' '
        if email !='':
            email_json = {"email":{
                          "@preferred":"true",
                          "@segment_type":"External",
                          "email_address":email,
                          "email_types":{
                              "email_type":"work"
                              }
                          }
                      } #end email_json

        barcode1_json = {"@segment_type":"External","id_type":"BARCODE","value":"250700%s" % str(data_dict['muid'])}
        barcode2_json = {"@segment_type":"External","id_type":"OTHER_ID_1","value":"250240%s" % str(data_dict['muid'])}
        identifier_count = 1
        tapID_array = data_dict['tapid']
        identifier_array = []
        if user_id != '':
            userid_json = {"@segment_type":"External","id_type":"UNIV_ID","value":user_id}
            identifier_array.append(userid_json)
        identifier_array.append(barcode1_json)
        identifier_array.append(barcode2_json)
        if pc2_str == "lawschool":
            barcode3_json = {"@segment_type":"External","id_type":"OTHER_ID_2","value":"252010%s" % str(data_dict['muid'])}
            campus_str = "lawlib"
        elif pc2_str == "medschool":
            barcode3_json = {"@segment_type":"External","id_type":"OTHER_ID_2","value":"210010%s" % str(data_dict['muid'])}
            campus_str = "medical"
            pc2_str = homelib_str
            cat2str = "medsite"
        else:
            campus_str ="main"
            cat2str = "unischool"

###     Med and Law barcodes    ###
        if barcode3_json != '0':
            identifier_array.append(barcode3_json)

###     Mobile Credential identifiers    ###
        if (tapID_array != [None]):
            for id in tapID_array:
                if (identifier_count == 1):
                    identifier_array.append({"@segment_type":"External","id_type":"OTHER_ID_3","value":str(id)})
                if (identifier_count == 2):
                    identifier_array.append({"@segment_type":"External","id_type":"OTHER_ID_4","value":str(id)})
                if (identifier_count == 3):
                    identifier_array.append({"@segment_type":"External","id_type":"OTHER_ID_5","value":str(id)})
                identifier_count += 1
        userident_json = {"user_identifiers":{"user_identifier":identifier_array}}

        emeriti = False
        for someone in self.emeriti_tuple:
            if int(someone['muid']) == int(muid):
                emeriti = True
        if emeriti:
            position = "emeritus"
        else:
            position = "facstaffadmin"

        homelib_catstr = "h" + homelib_str

        if cat2str != "":
            if pc3_str=="facstaffadmin":
                stat_json = {"user_statistics":{"user_statistic":[{
                    "@segment_type":"External",
                    "statistic_category":position,
                    "category_type":"uniempl"
                    },{
                    "@segment_type":"External",
                    "statistic_category":pc3_str,
                    "category_type":"uniempl"
                    },{
                    "@segment_type":"External",
                    "statistic_category":pc2_str,
                    "category_type":cat2str
                    },{
                    "@segment_type":"External",
                    "statistic_category":homelib_catstr,
                    "category_type":"homelib"
                    }]}}
            else:
                stat_json = {"user_statistics":{"user_statistic":[{
                    "@segment_type":"External",
                    "statistic_category":position,
                    "category_type":"uniempl"
                    },{
                    "@segment_type":"External",
                    "statistic_category":pc3_str,
                    "category_type":"nondegposition"
                    },{
                    "@segment_type":"External",
                    "statistic_category":pc2_str,
                    "category_type":cat2str
                    },{
                    "@segment_type":"External",
                    "statistic_category":homelib_catstr,
                    "category_type":"homelib"
                    }]}}
        else:
            stat_json = {"user_statistics":{"user_statistic":[{
                "@segment_type":"External",
                "statistic_category":position,
                "category_type":"uniempl"
                },{
                "@segment_type":"External",
                "statistic_category":pc3_str,
                "category_type":"nondegposition"
                },{
                "@segment_type":"External",
                "statistic_category":homelib_catstr,
                "category_type":"homelib"
                }]}}
  




        exp_date = self._formatDate(data_dict['expdate'])
        now = datetime.datetime.now()

        if emeriti:
            year = datetime.date.today().year + 1
            exp_date = datetime.datetime(year, 6, 30)
            exp_date = exp_date.strftime("%Y-%m-%d")

        if exp_date >= now.strftime("%Y-%m-%d"):
           status = "ACTIVE"
        else:
            status = "INACTIVE"

        exp_dateObj = datetime.datetime.strptime(exp_date,"%Y-%m-%d")
#        if pc2_str == "lawschool" and pc1_str =="3yrgrad" and exp_dateObj == datetime.date(2020,5,8):
#            exp_date = datetime.dateime.strftime(2020,11,1,"%Y-%m-%d")
        purge_date = self.add_years(exp_dateObj, 1)
        purge_date = datetime.datetime.strftime(purge_date,"%Y-%m-%d")

        user_role =[]
        if homelib_str == "law":
                user_role.append(self.patronRoleFill("law",exp_date))
        elif homelib_str == "musm":
                user_role.append(self.patronRoleFill("med",exp_date))
        elif homelib_str == "nav":
                user_role.append(self.patronRoleFill("nav",exp_date))
        elif homelib_str == "colm":
                user_role.append(self.patronRoleFill("colm",exp_date))
        elif homelib_str == "sav":
                user_role.append(self.patronRoleFill("sav",exp_date))
        if homelib_str != "nav":
                user_role.append(self.patronRoleFill("tarv",exp_date))
                user_role.append(self.patronRoleFill("swil",exp_date))
                user_role.append(self.patronRoleFill("hen",exp_date))
        if homelib_str == "tarv" or homelib_str == "swil" or homelib_str == "doug" or homelib_str == "hen":
                user_role.append(self.patronRoleFill("RES_SHARE",exp_date))
        user_role_json = {"user_roles":{"user_role":user_role}}





        patron_dict = {
                       "record_type":record_type,
                       "primary_id":patron_muid,
                       "first_name":firstName,
                       "last_name":lastName,
                       "middle_name":middleName,
                       "user_group":usergroup_str,
                       "campus_code":campus_str,
                       "preferred_language":preferred_lang,
                       "expiry_date":exp_date,
                       "purge_date":purge_date,
                       "account_type":"EXTERNAL",
                       "status": status,
                       }
        patron_dict.update(userident_json)
        patron_dict.update(stat_json)
        patron_dict.update(user_role_json)
        if email !='':
            contact_json = {"contact_info":[{"emails":[email_json]}]}
            patron_dict.update(contact_json)

        return patron_dict



    
    def getAllPatrons(self):
        '''
            Like the Perl version of this program, we query Postgres employee tables.
        '''
        if self.config['options'].getboolean('quiet')==False:
            sys.stderr.write("Retrieving patrons...")
        query = '''
                SELECT * FROM alma_employees
                '''


        emeritiQuery = '''SELECT muid from emeriti'''
        c = self.getCursor()
        c.execute(emeritiQuery)
        self.emeriti_tuple = c.fetchall()
        c.close()

        c = self.getCursor()
        c.execute(query)
        patron_dict = {}
        counter = 0
        dcounter=0
        for patron in c:
            if patron['muid'] in patron_dict:
                dcounter+=1
            patron['muid'] = int(patron['muid'])
            patron_dict[patron['muid']] = patron
            counter += 1
            if self.config['options'].getboolean('quiet')==False:
                if counter % 50 == 0:
                    sys.stderr.write('.')
        if self.config['options'].getboolean('quiet')==False:
            sys.stderr.write( "\nRetrieved %d patrons\n" % counter)
        self.patron_dict = patron_dict
