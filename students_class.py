#!/bin/python
#/var/www/virtual_webapps/alma_sis/bin/python
#/opt/rh/rh-python34/root/usr/bin/python
import psycopg, datetime, sys, smtplib, json
#import connect2db
import patron_class

class Students():
    con = None
    patron_dict = {}
    p_degree_255_counter = None
    p_degree_255 = None
    sender = 'py@mercer.edu'
    receivers = ['pham_cs@mercer.edu']
    message = """From: py@mercer.edu
To: pham_cs@mercer.edu
Subject: pc255 from py!

"""
    def __init__(self,options):
        '''
            Instantiate our Patrons object with access to command line options and
            arguments, as well as opening the database connection, and begin by 
            getting all the patrons.
        '''
        self.config = options
        self.p_degree_255_counter = 0
        self.p_degree_255 = set()

        dbhostname = self.config['patrons']['host']
        dbportnumber = int(self.config['patrons']['port'])
        dbname = self.config['patrons']['database']
        dbusername = self.config['patrons']['user']
        dbpassword = self.config['patrons']['password']

        self.con = self.connect2db(self, dbhostname, dbportnumber, dbname, dbusername, dbpassword)

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

    def _setpclass(self,p_class):
        '''
           Looks at the p_class data field, compares the value with the key in the dictionary.
           It saves and returns the value associated with the key. This value is related to
           the students' class (grade level).
        '''
        p_class=p_class.strip()
        p_classData_dict = {
                        "0-Freshman":"0freshman",
                        "1-Freshman":'freshman',
                        "2-Sophomore":"sophomore",
                        "3-Junior":"junior",
                        "4-Senior":"senior",
                        "Graduate - Master's":"masters",
                        "Graduate Specialist - Post Master's":"postmasters",
                        "1st Year Graduate/Professional":"1yrgrad",
                        "2nd Year Graduate/Professional":"2yrgrad",
                        "3rd Year Graduate/Professional":"3yrgrad",
                        "4th Year Graduate/Professional":"4yrgrad",
                        "Doctoral Candidate":"drcandid",
                        "Auditor - no credit":"auditor",
                        "Cross Registered Undergraduate":"crossugrad",
                        "Cross Registered Graduate/Professional":"crossgradprof",
                        "High School":"highschool",
                        "International Exchange Student":"international",
                        "Special Non-Degree Graduate":"specnodgrad",
                        "Special Non-Degree Undergrad":"specnondugrad",
                        "Transient Professional":"transientprof",
                        "Transient Undergraduate":"transugrad",
                        "Transient Graduate":"transientgrad",
                        "Transient Graduate/Professional":"transientgrad",
                        "Non Credit":"noncredit",
                        "Special Non-Degree Professional":"specnodprof",
                        "Special Non-Degree Graduate/Professional":"specnodprof",
                        "Special Preparatory Undergrad - 1":"specprepugrad1",
                        }
        pclass1=None
        if p_class in p_classData_dict:
            pclass1=p_classData_dict[p_class]

        else:
            pclass1="blankclass"
        return pclass1



    def _setpschool(self,p_school):
        p_school=p_school.strip()
        p_schoolData_dict = {
                        "College of Health Professions":"cohp",
                        "College of Liberal Arts":"cla",
                        "College of Liberal Arts and Sciences":"cla",
                        "College of Pharmacy":"cop",
                        "Eugene W. Stetson School of Business & Economics":"stetson",
                        "Stetson-Hatcher School of Business":"stetson",
                        "Stetson":"stetson",
                        "Georgia Baptist College of Nursing":"nursing",
                        "James & Carolyn McAfee School of Theology":"theology",
                        "Penfield College":"ccps",
                        "College of Professional Advancement":"ccps",
                        "College of Continuing and Professional Studies":"ccps",
                        "School of Engineering":"engineering",
                        "School of Medicine":"medschool",
                        "Tift College of Education":"tift",
                        "Townsend School of Music":"music",
                        "Walter F. George School of Law":"lawschool",
                        "English Language Institute":"eli",
                        "Cross Registered Professional":"crossprof",
                        "Special Non-Degree Professional":"specnondegpschool",
                        "Continuing Education":"continueed",
                        }
        pschool2=None
        if p_school in p_schoolData_dict:
            pschool2=p_schoolData_dict[p_school]

        else:
            pschool2="blankschool"
        return pschool2



    def _setpdegree(self,p_degree):
        p_degree=p_degree.strip()
        p_degreeData_dict = {
                        "Bachelor of Applied Science":"bapplsci",
                        "Bachelor of Arts":"barts",
                        "Bachelor of Arts.":"barts",
                        "Bachelor of Fine Arts":"bfarts",
                        "Bachelor of Business Admin.":"bbusiadmin",
                        "Bachelor of Business Admin":"bbusiadmin",
                        "Bachelor of Liberal Studies":"blibstud",
                        "Bachelor of Music":"bmusic",
                        "Bachelor of Music Education":"bmusiced",
                        "Bachelor of Science":"bscience",
                        "Bachelor of Science in Education":"bseduc",
                        "Bachelor of Science in Nursing":"bsnursing",
                        "Bachelor of Science in Pharmaceutical Sciences":"bspharmsci",
                        "Bachelor of Science in Social Science":"bssocialsci",
                        "B.S. in Engineering":"bsengineering",
                        "B.S. in Health Science":"bshealthsci",
                        "Doctor of Medicine":"docmed",
                        "Doctor of Ministry":"docmin",
                        "Doctor of Nursing Practice":"docnursprac",
                        "Doctor of Pharmacy":"docpharm",
                        "Doctor of Philosophy":"docphilo",
                        "Doctor of Physical Therapy":"docphystherap",
                        "Doctor of Psychology":"docpsych",
                        "Doctor of Public Health":"docpubhealth",
                        "Executive Master of Bus. Admin.":"execmba",
                        "Juris Doctor":"jurisdoc",
                        "Doctor of Medicine 3-yr Accelerated":"docmed3yracc",
                        "Master of Accountancy":"maccount",
                        "Master of Arts":"marts",
                        "Master of Arts in Teaching":"martsteach",
                        "Master of Arts in Teaching Pedagogy Only-Atlanta Campus":"martsteach",
                        "Master of Arts in Teaching Pedagogy Only":"martsteach",
                        "Master of Bus. Admin./Master of Acct.":"mbamastacct",
                        "Master of Business Admin.":"mbusiadmin",
                        "Master of Business Admin":"mbusiadmin",
                        "Grad Direct Master of Business Admin.":"mbusiadmin",
                        "Grad Direct Master of Business Admin":"mbusiadmin",
                        "Master of Divinity":"mdivinity",
                        "Master of Education":"meducation",
                        "Master of Family Therapy":"mfamtherap",
                        "Master of Laws":"mlaws",
                        "Master of Medical Science":"mmedsci",
                        "Master of Music":"mmusic",
                        "Master of Public Health":"mpubhealth",
                        "Master of Science":"msci",
                        "Grad Direct Master of Science":"msci",
                        "Master of Science in Biomedical Sciences":"msbiomedsci",
                        "Master of Science in Nursing":"msnursing",
                        "Master of Science in Preclinical Sciences":"mspreclinsci",
                        "Master of Science in Health Outcomes":"mshealthoutcomes",
                        "M.S. in Engineering":"msengineer",
                        "Non-Degree Graduate":"nondeggrad",
                        "Non-Degree Graduate Artist Diploma":"nondeggradart",
                        "Non-Degree Professional":"nondegprof",
                        "Non-Degree Undergraduate":"nondegugrad",
                        "Non-Degree Undergraduate Joint Enrollment -  CPV - Atlanta Center":"nondegugrad",
                        "Non-Degree Undergraduate Oglethorpe/Tift":"nondegugrad",
                        "Non-Credit":"degnoncredit",
                        "Non-Degree Undergraduate Joint Enrollment":"nondegugjoint",
                        "Professional Master of Bus. Admin.":"profmba",
                        "Specialist in Education":"speceduc",
                        "Non-Credit Program":"degnoncredit",
                        "Bachelor of Science in Nursing - RN/BSN1":"bsnursing",
                        "Non-Degree Professional - Walter F. George School of Law":"nondegproflaw",
                        "Professional Certificate in Rehabilitation Services":"procertrehabserv",
                        "Postsecondary Professional Certificate in Rehabilitation Services":"procertrehabserv",
                        "Two Year Master of Bus. Admin.":"mbusiadmin",
                        "MG Non Degree MSB Map":"nondegmsbmap",
                        "MG Non-Degree MBA Map":"nondegmsbmap",
                        "MG Non-Degree MBA Map - BUS- Atlanta Campus":"nondegmsbmap",
                        "Post-Master's Doctor of Nursing Practice":"docnursprac",
                        "Non-Degree Graduate Health Professions":"nondeggradhp",
                        "Doctor of Philosophy EDU - Atlanta Campus":"docphiloedu",
                        "Professional Master of Business Administration for Innovation":"profmbainno",
                        "Certificate in Theological Studies":"certtheostud",
                        "Certificate in Christian Social Enterprise":"certchristsocente",
                        "Post Master's Certificate in Marriage & Family Therapy":"postmastcertmft",
                        "Juris Doctor - Walter F. George School of Law":"jurisdoc",
                        "Specialist in Education - Distance Learning":"speceduc",
                        "M.S. in Engineering Yr 5":"msengineeryr5",
                        "Master of Science Yr 5":"msciyr5",
                        "Postbaccalaureate Certificate in Theological Studies":"certchristsocente",
                        "Master of Theological Studies":"mtheostud",
                        "Master of Athletic Training":"mathltrain",
                        "Master of Science in Pharmaceutical Sciences":"mspharmsci",
                        "Postbaccalaureate Certificate in Health Informatics":"postbcerthealinfo",
                        "Postbaccalaureate Certificate":"postbacccert",
                        "Preparatory Undergraduate Program Pharmacy":"prepugradpharm",
                        "Prepartory Undergraduate Program ABSN":"prepugradnurs",
                        "Preparatory Undergraduate Program ABSN":"prepugradnurs",
                        "Preparatory Undergraduate Program Nursing":"prepugradnurs",
                        "Post-Master's Certificate for Adult-Gerontology Acute Care Nurse Practitioner":"postcertnurseprac",
                        }
        pcode3=None
        if p_degree in p_degreeData_dict:
            pdegree3=p_degreeData_dict[p_degree]

        else:
            pdegree3="blankdegree"
            self.p_degree_255_counter += 1
            self.p_degree_255.add(pdegree3)
        return pdegree3
    def _sethlib(self,hlib):
        hlib=hlib.strip()
        hlibData_dict = {
                         "Atlanta Campus":"atlmed",
                         "Atlanta Center":"swil",
                         "Atlanta - Non-Credit":"swil",
                         "Columbus":"colm",
                         "Distance Learning":"disl",
                         "Douglas County":"doug",
                         "Eastman":"tarv",
                         "Forsyth County":"swil",
                         "Henry County":"hen",
                         "Macon Center":"tarv",
                         "Macon Main":"tarv",
                         "Medical School-Macon":"musm",
                         "Medical Center of Central GA - Macon":"medcent",
                         "Navicent Health  Macon":"nav",
                         "Navicent Health - Macon":"nav",
                         "Newnan Center":"swil",
                         "Piedmont Hospital":"pied",
                         "Savannah":"sav",
                         "Walter F. George School of Law":"law",
                         }
        homelibr=None
        if hlib in hlibData_dict:
            homelibr=hlibData_dict[hlib]

        else:
            homelibr="zzzzz"
        return homelibr


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
        Convert row of patron data into dictionary for student_sis_loader to convert to xml for Alma SIS load
        '''

        data_dict = self.patron_dict[muid]
        p_class_str = self._setpclass(data_dict['p_class'])
        p_school_str = self._setpschool(data_dict['p_school'])
        p_degree_str = self._setpdegree(data_dict['p_degree'])


        nameSplit1 = data_dict['name'].split(',')
        name_len = len(nameSplit1)
        if name_len > 1:
            nameSplit2 = nameSplit1[1]
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

        exp_date = self._formatDate(data_dict['expdate'])
        now = datetime.datetime.now()
#        in_feed = data_dict['in_feed']
#        if (exp_date >= now.strftime("%Y-%m-%d") or in_feed=='t'):
        if exp_date >= now.strftime("%Y-%m-%d"):
           status = "ACTIVE"
        else:
            status = "INACTIVE"
        
        exp_dateObj = datetime.datetime.strptime(exp_date,"%Y-%m-%d")
        if p_school_str == "lawschool" and p_class_str =="3yrgrad" and exp_dateObj == datetime.date(2020,5,8):
            exp_date = datetime.dateime.strftime(2020,11,1,"%Y-%m-%d")
        purge_date = self.add_years(exp_dateObj, 1)
        purge_date = datetime.datetime.strftime(purge_date,"%Y-%m-%d")
        

        account_type = "External"
        preferred_lang ="en"
        record_type = "PUBLIC"
        patron_muid = str(data_dict['muid'])
        
        addresses = []
        address1 = False
        if (data_dict['address1'].split('$') != ''):
            addressSplit1 = data_dict['address1'].split('$')
            addressSplit2 = addressSplit1[1].split(', ')
            city = addressSplit2[0]
            aftercity = addressSplit2[1]
            state = aftercity.split(' ')[0]
            zipcode = aftercity.split(' ')[1]
            line1 = addressSplit1[0].split('  ')[0]

            address_note = data_dict['address2']
            if (address_note != ''):
                address_json = {"address":{"@preferred":"true",
                        "@segment_type":"External",
                        "line1":line1,
                        "city":city,
                        "state_province":state,
                        "postal_code":zipcode,
                        "address_note": address_note,
                        "address_types":{
                            "address_type":"home"
                            }
                          }
                       }
            else:
                address_json = {"address":{"@preferred":"true",
                        "@segment_type":"External",
                        "line1":line1,
                        "city":city,
                        "state_province":state,
                        "postal_code":zipcode,
                        "address_types":{
                            "address_type":"home"
                            }
                          }
                       } #end address_json
            address1 = True
        if address1 == False:
            address_json = {}

        addresses.append(address_json)

        email_json = {"email":{
                          "@preferred":"true",
                          "@segment_type":"External",
                          "email_address":data_dict['email'],
                          "email_types":{
                              "email_type":"school"
                              }
                          }
                      } #end email_json

        if data_dict['phone1']!='':
            phone1_json = {"phone":{
                               "@preferred":"true",
                               "@preferred_sms":"false",
                               "@segment_type":"External", 
                               "phone_number":data_dict['phone1'],
                               "phone_types":{
                                   "phone_type":"home"
                                   }
                               }
                          } #end phone1_json
            phone1 = True
        else:
            phone1 = False            

        if data_dict['phone2']!='':
            phone2_json = {"phone":{
                               "@preferred":"false",
                               "@preferred_sms":"false",
                               "@segment_type":"External", 
                               "phone_number":data_dict['phone2'],
                               "phone_types":{
                                   "phone_type":"home"
                                   }
                               }
                          } #end phone2_json
            phone2 = True
        else:
            phone2 = False

        muid_json = {"@segment_type":"External","id_type":"UNIV_ID","value":str(data_dict['muid'])}
        barcode2_json = "0"
        barcode3_json = "0"
        campuscode_str = ""
        usergroup_str = "999"
        homelib_str = self._sethlib(data_dict['hlib']).lower()
        homelib_catstr = "h" + homelib_str
        if p_school_str == "lawschool":
            # Law student
            barcode3_json = {"@segment_type":"External","id_type":"OTHER_ID_2","value":"252010%s" % str(data_dict['muid'])}
            campuscode_str = "lawlib"
            usergroup_str = "lstud"
            if p_class_str != "blankclass":
                p_class_str = p_class_str + "law"
                lawclassstr = "lawclass"
                if homelib_catstr != "hlaw":
                    homelib_catstr = "hlaw"
            else:
                lawclassstr = "uniclass"
            p_degree_str = self._setpdegree(data_dict['p_degree'])
            if p_degree_str != "blankdegree":
                lawdegreestr = "lawdegree"
            else:
                lawdegreestr = "unidegree"
            stat_json = {"user_statistics":{"user_statistic":[{
                "@segment_type":"External",
                "statistic_category":p_class_str,
                "category_type":lawclassstr
                },{
                "@segment_type":"External",
                "statistic_category":p_degree_str,
                "category_type":lawdegreestr
                },{
                "@segment_type":"External",
                "statistic_category":homelib_catstr,
                "category_type":"homelib"
                }]} }
        elif p_school_str == "medschool":
            # Med student
            if homelib_str == "sav":
                barcode3_json = {"@segment_type":"External","id_type":"OTHER_ID_2","value":"21003%s" % str(data_dict['muid'])}
            else:
                barcode3_json = {"@segment_type":"External","id_type":"OTHER_ID_2","value":"210010%s" % str(data_dict['muid'])}
            campuscode_str = "medical"
            if (p_class_str =="1yrgrad" or p_class_str=="2yrgrad"):
                usergroup_str = "stud12"
            elif (p_class_str =="3yrgrad" or p_class_str=="4yrgrad"):
                usergroup_str = "stud34"
            else:
                usergroup_str = "astphd"

            if p_class_str != "blankclass":
                p_class_str = p_class_str + "med"
                medclassstr = "medclass"
            else:
                medclassstr = "uniclass"
            if p_degree_str != "blankdegree":
                meddegreestr = "meddegree"
            else:
                meddegreestr = "unidegree"
            if homelib_str != "zzzzz":
                p_school_str = homelib_str
                medsitestr = "medsite"
            else:
                medsitestr = "unischool"

            if (p_degree_str=="msbiomedsci" or p_degree_str=="mfamtherap" or p_degree_str=="mspreclinsci"):
                usergroup_str = "mmastphd"
            elif p_school_str == "nav":
                usergroup_str = "mnstud"
            elif p_school_str == "sav":
                usergroup_str = "ms" + usergroup_str
            elif p_school_str == "colm":
                usergroup_str = "mc" + usergroup_str
            else:
                usergroup_str = "mm" + usergroup_str


            stat_json = {"user_statistics":{"user_statistic":[{
                "@segment_type":"External",
                "statistic_category":p_class_str,
                "category_type":medclassstr
                },{
                "@segment_type":"External",
                "statistic_category":p_degree_str,
                "category_type":meddegreestr
                },{
                "@segment_type":"External",
                "statistic_category":p_school_str,
                "category_type":medsitestr
                },{
                "@segment_type":"External",
                "statistic_category":homelib_catstr,
                "category_type":"homelib"
                }]}}
        else:
            # Main student
            campuscode_str = "main"
            usergroup_str = "ustud"
            if homelib_catstr == "hatlmed":
                homelib_catstr = "hswil"
            stat_json = {"user_statistics":{"user_statistic":[{
                               "@segment_type":"External",
                               "statistic_category":p_class_str,
                               "category_type":"uniclass"
                               },{
                               "@segment_type":"External",
                               "statistic_category":p_school_str,
                               "category_type":"unischool"
                               },{
                               "@segment_type":"External",
                               "statistic_category":p_degree_str,
                               "category_type":"unidegree"
                               },{
                               "@segment_type":"External",
                               "statistic_category":homelib_catstr,
                               "category_type":"homelib"
                               }]}}

        barcode1_json = {"@segment_type":"External","id_type":"BARCODE","value":"250700%s" % str(data_dict['muid'])}
        barcode2_json = {"@segment_type":"External","id_type":"OTHER_ID_1","value":"250240%s" % str(data_dict['muid'])}
        identifier_count = 1
        tapID_array = data_dict['tapid']
        identifier_array = []
        identifier_array.append(barcode1_json)
        identifier_array.append(barcode2_json)

###     Med and Law barcode        ###
        if barcode3_json != '0':
            identifier_array.append(barcode3_json)


###     Mobile Credential identifiers        ####
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

        if phone2 and phone1==False:
           phone_json = [phone2_json]
        elif phone2 and phone1:
           phone_json = [phone1_json, phone2_json]
        elif phone1:
           phone_json = [phone1_json]
        else:
           phone_json = []

        user_role =[]
        if homelib_str != "zzzzz":
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
                       "record_type":"PUBLIC",
                       "primary_id":patron_muid,
                       "first_name":firstName,
                       "last_name":lastName,
                       "middle_name":middleName,
                       "user_group":usergroup_str,
                       "campus_code":campuscode_str,
                       "preferred_language":preferred_lang,
                       "expiry_date":exp_date,
                       "purge_date":purge_date,
                       "account_type":account_type,
                       "status": status,
                       "contact_info":[{
                           "addresses":addresses,
                           "emails":[email_json],
                           "phones":phone_json,
                           }],
                       }
        patron_dict.update(userident_json)
        patron_dict.update(stat_json)
        patron_dict.update(user_role_json)

        return patron_dict
    
    def getAllPatrons(self):
        '''
            Get patrons from patron db.
        '''
        if self.config['options'].getboolean('quiet')==False:
            sys.stderr.write("Retreiving patrons...")
        query = '''
                SELECT *
                FROM
                    alma_patrons 
                WHERE (expdate + interval '1 month') > (now()::date - 14) AND
                p_school != 'Mercer College for Kids'
                ORDER BY muid
               '''
        c = self.getCursor()
        c.execute(query)
        patron_dict = {}
        counter = 0
        for patron in c:
            patron['muid'] = int(patron['muid'])
            patron_dict[patron['muid']] = patron
            #patron['expdate'] = self._formatDate(patron['expdate'])
            #print "%d: %s" %(patron['muid'],patron_dict[patron['muid']])
            counter += 1
            if self.config['options'].getboolean('quiet')==False:
                if counter % 50 == 0:
                    sys.stderr.write('.')
        if self.config['options'].getboolean('quiet')==False:
            sys.stderr.write( "\nRetrieved %d patrons\n" % counter)
        self.patron_dict = patron_dict
    
