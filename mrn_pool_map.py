#!/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python

# -*- coding: utf-8 -*-
import os
import sys
import csv
import argparse
import re
import subprocess
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb


def getCVRDBHandle(logger):
    dbObj = None
    propObj = Properties()
    logger.debug( "%s - %s - %s"%(propObj.getCVRDBUser(),propObj.getCVRDBServer(),propObj.getCVRDBName()))
    try:
        dbObj = DMPdb(
                       propObj.getCVRDBUser(),
                       propObj.getCVRDBPass(),
                       propObj.getCVRDBServer(),
                       propObj.getCVRDBName()
                     )
    except Exception as e:
        logger.error(e)
        dbObj = None
        raise("Database handle not created!")
    return dbObj

def sql_cursor_exe(dbObj,sql):
    dbObj.execute(sql) #bool
    rec = dbObj.fetchall() #tuple with each dict element
    # print the column names
    if rec:
        #print_formatted_columns(rec[0].keys())
        for r in rec:
            print_formatted_columns(r.values())
    else:
        pass


def print_formatted_columns(l):
    print '\t'.join('{}'.format(el) for el in l)

def csv_reader2list(file,is_csv=True):
	readerObj = None
	records_list= None
	if os.path.exists(file):
		try:	 # head reader is regualar reading file
	    	     readerObj =csv.reader(open(file,"rU"),delimiter='\t', quotechar='"') if is_csv else open(file,'rU')
	    	     records_list = list(readerObj) # store each row record into list
		except Exception, e:
			 #raise("Can't read file to obj")
			 print e
	return records_list

def getMRNPOOL(SampleIDF):
	 #get  sampleID from de_identifed file and query within CVR tables
 	logger = DMPLogger()
 	logger = logger.getCVRLogger("TestDB")
 	logger.debug("Creating database connection")
 	dbObj = None
 	try:
		dbObj = getCVRDBHandle(logger)
 		dbObj.connect()
		logger.debug("Successful connection")
		#rl= csv_reader2list(SampleIDF,False) #read file of generic sample_id to list
		#logger.debug(rl)
		head = ['POOL_NAME','MRN','SAMPLE_ID','PATIENT_ID',]
		print_formatted_columns(head)
		sqlGetMRNPool= """
			SELECT
           				dp.dmp_patient_lbl AS PATIENT_ID ,
            				ds.dmp_sample_lbl AS SAMPLE_ID ,
           			 	at.dmp_alys_task_name,
            				dp.mrn
        			FROM
            				dmp_alys_task at
        			LEFT JOIN
            				dmp_dms dms
        			ON
        				(at.dmp_dms_id=dms.dmp_dms_id)
        			LEFT JOIN
            				alys2sample a2s
        			ON 	(at.dmp_alys_task_id=a2s.dmp_alys_task_id)
       			LEFT JOIN
            				dmp_sample ds
        			ON
        				(a2s.dmp_sample_id=ds.dmp_sample_id)
        			LEFT JOIN
            				dmp_patient dp
        			ON
        				(ds.dmp_patient_id=dp.dmp_patient_id)
        			WHERE
        				ds.dmp_sample_lbl = 	'%s'
        			ORDER BY
        				 at.dmp_alys_task_id
        			"""
		sql = sqlGetMRNPool.replace("\n", " ")
		fr = open(SampleIDF,'rU')
		for r in fr:
			r= r.replace('\n','')
			#logger.debug(sql%r)
			sql_cursor_exe(dbObj,sql%r)
	except Exception as e :
        		logger.error(e)
	finally:
        		if dbObj:
            			dbObj.disconnect()
	logger.debug("Completed testing!")

def main():
	# Setting up argparse,accepting user input arguments to generate command line
	parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")

	parser.add_argument('-f','--file_name',action='store', dest='file',
                    required=True, help='***Mapping MRN and Pool Name *** \n'+
                                'Please give the file name that needs to be fetch\n')

    	user_args = parser.parse_args()
    	f= user_args.file
    	getMRNPOOL(f)

if __name__ == '__main__':
	main()