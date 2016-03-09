#!/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import csv
import argparse
import re
import inspect
import subprocess
import json,requests,ast
from os.path import basename
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
#from app.lib.cvr.ui.utils.ui_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb
import urllib2,requests,json
from requests.auth import HTTPDigestAuth
import urllib2, base64
from base64 import b64encode
import httplib

class MRN(object):
	dbObj = None
	mrn = None
	legacy_patient_lbl = None
	dmp_patient_lbl  = None

	def __init__(self,mrn):
		self.mrn = mrn

	def getMRN(self):
		return self.mrn


	def getCVRDBHandle(self):
		propObj = Properties()
		logger = DMPLogger()
		dbObj = None
		logger = logger.getCVRLogger("getCVRDBHandle")
 		logger.debug("Creating database connection for dmp_patient for a sample ")
 		logger.debug(propObj.config_file)
 		logger.debug(inspect.getfile(Properties) )
 		logger.debug( "%s - %s - %s-%s"%(propObj.getCVRDBUser(),propObj.getCVRDBServer(),propObj.getCVRDBName(),propObj.getCVRDBPass()))
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

    	def validateMRN(self):
		if len(self.mrn) !=8:
			raise ValueError("Invalid mrn length %s  in table " % self.mrn)

    	def getALLFromPateints(self):
    		logger = DMPLogger()
		logger = logger.getCVRLogger("getPIMeta")
		dbObj= self.getCVRDBHandle()
		dbObj.connect()
		try:
 			sql = 'select * from dmp_patient where char_length(mrn) !=8 '
 			logger.debug(sql)
 			dbObj.execute(sql)
 			ret = dbObj.fetchall
 			logger.debug(ret)
 		except Exception, e:
 			raise
 		else:
 			pass
 		finally:
 			pass




def main():
	# Setting up argparse,accepting user input arguments to generate command line
	parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")

	parser.add_argument('-m','--mrn',action='store', dest='mrn',
                    required=True, help='***mrn*** \n'+
                                'Please give the mrn that needs to be loaded\n')
	user_args = parser.parse_args()

	m=user_args.mrn
	mOBJ = MRN(m)
	print mOBJ.getMRN(), mOBJ.getALLFromPateints()
	print mOBJ.validateMRN()

if __name__ == '__main__':
	main()

