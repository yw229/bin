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

class A2SStatus(object):
	dbObj = None
	alys2sample_id = None
	nonRegisteredA2SList = None
	nonSoMrevList = None

	def __init__(self,alys2sample_id=None):
		self.alys2sample_id = alys2sample_id
		self.nonRegisteredA2SList = list()
		self.nonSoMrevList = list()


	def getA2SId(self):
		return self.alys2sample_id

	def getNonSignOutA2S(self):
		return tuple(self.nonRegisteredA2SList)

	def getNonSignOutMrev(self):
		return self.nonSoMrevList

	def getCVRDBHandle(self):
		propObj = Properties()
		logger = DMPLogger()
		logger = logger.getCVRLogger("getCVRDBHandle")
 		logger.debug("Creating database connection for Alys2Sample Status change ")
 		logger.debug(propObj.config_file)
 		logger.debug(inspect.getfile(Properties) )
 		logger.debug( "%s - %s - %s-%s"%(propObj.getCVRDBUser(),propObj.getCVRDBServer(),propObj.getCVRDBName(),propObj.getCVRDBPass()))
   		try:
        			self.dbObj = DMPdb(
                       	propObj.getCVRDBUser(),
                       	propObj.getCVRDBPass(),
                       	propObj.getCVRDBServer(),
                       	propObj.getCVRDBName()
			)
		except Exception as e:
        			logger.error(e)
        			self.dbObj = None
        			raise("Database handle not created!")
    		return self.dbObj

    	def checkSingleSoStatus(self):
    		logger = DMPLogger()
		logger = logger.getCVRLogger("exists_in_manual_review")
		logger.debug("Check if alys2sample_id is registered in dmp_sample_mrev  table")
		dbObj = self.getCVRDBHandle()
 		dbObj.connect() #need to connect to the cvr db
 		retVal = True
 		try:
 			SQLCheckMrevStatus='''SELECT * FROM dmp_sample_mrev WHERE alys2sample_id = %s'''
 			dbObj.execute(SQLCheckMrevStatus%self.getA2SId())
 			ret = dbObj.fetchone()
 			logger.debug('ret %s'%ret)
 			if not ret: #validate a2s is created in DB
 				logger.debug('Alys2Sample %s is not created in CVR DB'%self.getA2SId())
 				ret = False
 				raise ValueError( '%s is not created in DB'%self.getA2SId() )
 			else: #
 				SQLCheckSOStatus = '''SELECT * FROM
 					dmp_sample_mrev mr JOIN dmp_sample_so so
 					 ON mr.dmp_sample_mrev_id = so.dmp_sample_mrev_id
 					 WHERE alys2sample_id =%s'''
 				soRet = dbObj.fetchone()
 				logger.debug('soRet%s'%soRet)
 				retVal = True if soRet else False

 		except Exception as e:
 			logger.error(e)
 			dbObj = None
 			retVal = False
 			raise ValueError('Error Alys2Sample %s  is not in DB'%self.getA2SId())
		return retVal

	def filterA2S(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("filterA2S")
		logger.debug("Filter Out alys2sample_id registered in dmp_sample_mrev but not in Sign Out table")
		dbObj = self.getCVRDBHandle()
 		dbObj.connect() #need to connect to the cvr db
 		retVal = True
 		try:
 			#Alys2sample that are in dmp_sample_mrev table but not registered into dmp_sample_so
 			SQLCheckSOStatus ='''	SELECT mr.alys2sample_id,mr.mrev_status_cv_id,mr.dmp_sample_mrev_id AS mr_id, so.dmp_sample_mrev_id
 						FROM dmp_sample_mrev mr LEFT JOIN dmp_sample_so so
 					 	ON  mr.dmp_sample_mrev_id = so.dmp_sample_mrev_id where so.dmp_sample_mrev_id  is  NULL '''
 			dbObj.execute(SQLCheckSOStatus)
 			soRet = dbObj.fetchall() #tuple
 			#logger.debug(soRet)
 			for e in soRet:
 				if e['alys2sample_id'] not in self.nonRegisteredA2SList:
 					self.nonRegisteredA2SList.append( int(e['alys2sample_id']) )
 				if e['mr_id'] not in self.nonSoMrevList:
 					self.nonSoMrevList.append( int(e['mr_id'] ) )
 			logger.debug(len(self.nonSoMrevList))
 			logger.debug(len(self.nonRegisteredA2SList))
 			retVal = True if len(self.nonRegisteredA2SList) >0 and len(self.nonSoMrevList) >0 else False
 		except Exception as e:
 			logger.error(e)
 			dbObj = None
 			retVal = False
 			raise ValueError('Error Alys2Sample for filterA2S ')
		return retVal

	def registerMrvComplete(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("registerMrvStatus")
		logger.debug("move status of such entries in the dmp_sample_mrev to manual review complete")
		retVal = True
		dbObj = self.getCVRDBHandle()
 		dbObj.connect()
		try:
			if  self.filterA2S():
				SQLUpdateMre2Complate = ''' UPDATE dmp_sample_mrev SET mrev_status_cv_id = 3  WHERE  alys2sample_id in %r'''
				logger.debug( SQLUpdateMre2Complate%(self.getNonSignOutA2S(),)) #getNonSignOutA2S is tuple
				dbObj.execute(SQLUpdateMre2Complate%(self.getNonSignOutA2S(),))
				retVal = True
			else:
				#logger.error('There is no alys2sample entries that are in manual review but not registered in dmp_sample_so table ! ')
				raise ValueError('There is no alys2sample entries that are in manual review but not registered in dmp_sample_so table ! ')
		except Exception as e :
			logger.error(e)
 			retVal = False
 			raise RuntimeError("There is no alys2sample entries that are in manual review but not registered in dmp_sample_so table ")

 		#if retVal:
 		#	print 'Registered Alys2sample Entries to complete !'
 		return retVal

 	def addSOReady(self):
 		logger = DMPLogger()
		logger = logger.getCVRLogger("addSOReady")
		logger.debug("register status of such entries in the dmp_sample_so to 1")
		retVal = True
		dbObj = self.getCVRDBHandle()
 		dbObj.connect()
 		try:
 			SQLInsertSoStatus = '''INSERT INTO dmp_sample_so(dmp_sample_mrev_id,pathologist_cv_id,so_status_cv_id) VALUES (%s,%s,%s)'''
 			if self.registerMrvComplete():
 				for ele in self.nonSoMrevList:
 					logger.debug(SQLInsertSoStatus%(ele,1,1))
 					dbObj.execute(SQLInsertSoStatus%(ele,1,1))
 		except Exception as e :
			logger.error(e)
 			retVal = False
 			raise RuntimeError("Error in register entries in dmp_sample_so !")

 		if retVal:
 			print 'Registered Alys2sample Entries in Sign Out Table complete !'
 		return retVal




def main():
	# Setting up argparse,accepting user input arguments to generate command line
	parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")

	parser.add_argument('-a','--alys2sample_id',action='store', dest='alys2sample_id',
                    required=False, help='***Checking Status for Alys2Sample object*** \n'+
                                'Please give the a2sid that needs to be loaded\n')
	user_args = parser.parse_args()

	try :
		a2sid = user_args.alys2sample_id
	except:
		a2sid = ''
	status = A2SStatus(a2sid)
	#print status.checkSingleSoStatus()
	print status.filterA2S() #status.getNonSignOutA2S()
	status.registerMrvComplete()
	#print status.getNonSignOutMrev()
	status.addSOReady()

if __name__ == '__main__':
	main()

