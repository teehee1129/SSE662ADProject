from multiprocessing import Pool
import datetime, base64, requests, sys, json, os, argparse, re, psycopg
import traceback
from configparser import ConfigParser
#from psycopg2.extras import DictCursor
from employee_loader import Employees
from xmljson import badgerfish as bf
from xml.etree.ElementTree import Element, fromstring, tostring

class PatronLoader(object):
    debug = True
    auth_token = None
    config = None
    offset_index = 0
    outdir = "/usr/local/almasisload/synchronize/"

    
    def __init__(self, auth_token=None, config=None):
        self.config = config
    
    
    def _conditionally_print(self,counter,total):
        if counter % int(total*0.01) == 0:
            sys.stderr.write('.')
        elif counter % int(total*0.1) == 0:
            sys.stderr.write(" {}0% ".format( int(counter/(int(total*.1)))) )
    
    

    def _db_connect(self):
        '''
        Connect to Mercer Library Patron database
        '''
        """
        con = psycopg2.connect(
                               database=self.config['patrons']['database'],
                               host=self.config['patrons']['host'],
                               port=int(self.config['patrons']['port']),
                               user=self.config['patrons']['user'],
                               password=self.config['patrons']['password'])
        cur = con.cursor(cursor_factory=DictCursor)
        """

        hostname = self.config['patrons']['host']
        dbname = self.config['patrons']['database']
        portnum = int(self.config['patrons']['port'])
        username = self.config['patrons']['user']
        password = self.config['patrons']['password']


        con = psycopg.connect('host=%s port=%s dbname=%s user=%s password=%s'%(hostname, portnum, dbname, username, password))

        cur = con.cursor(row_factory=psycopg.rows.dict_row)

    
        return {'con':con,'cur':cur}
    
    def get_existing_patrons(self, id=None):
        '''
        Retrieve current employees from the patron db by calling the employees_loader.py functions creates xml file for alma sis
        '''
        counter = 1
        with Pool(processes = 4, maxtasksperchild = 50) as pool:
            results = []
            new_counter = 0
            existing_counter = 0
            employees = Employees(options=self.config)
            total_employees = len(employees.patron_dict)
            print("Retrieved {} employee records".format(total_employees))
            
            for muid in employees.patron_dict:
                results.append(employees.get_json(muid))
                new_counter += 1;
            pool.close()
            pool.join()
            today = str(datetime.date.today())
            outfile = self.outdir + "empl-" + today +".xml"
            xml4 = bf.etree({"user":results}, root=Element ('users'))
            xml4 = tostring(xml4)
            output = open(outfile,"wb")
            output.write(xml4)
            print ("Employee count: ", new_counter)
if __name__ == '__main__':
    base_dir = re.sub(r'(.*)employee_sis_loader.py',r'\1',os.path.realpath(__file__))    
    default_configfile = os.path.join(base_dir, 'config.cfg')
    parser = argparse.ArgumentParser(description='Update existing patrons and load new ones.')
    parser.add_argument('--config', 
                        default=default_configfile,
                        help='location of the config file')
    parser.add_argument('--run', action='store_true',
                        help="Update all existing, and find new patron records to load.")
    parser.add_argument('id',nargs='*',help='Patron IDs - you can repeat this')
    program_args = parser.parse_args()
    
    config = ConfigParser()
    config.read( default_configfile )
    
    if len(program_args.id) > 0:
        sys.stderr.write("Arguments received: {}\n".format(program_args.id))
        rest = PatronLoader( config=config )
        rest.get_patron_record(muid = program_args.id[0])
    elif program_args.run ==True:
        rest = PatronLoader(config=config)
        start = datetime.datetime.now()
        rest.get_existing_patrons()
        print("Processed all patrons in {}".format(datetime.datetime.now()-start))
    else:
        parser.print_help()
    #print("JSON output: \n{}\n".format(json.dumps(result, indent=4, sort_keys=True)))
    
