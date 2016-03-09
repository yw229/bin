#!/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import csv
import argparse
import re
import subprocess
import json,requests,ast
from os.path import basename
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb
import urllib2,requests,json
from requests.auth import HTTPDigestAuth
import urllib2, base64
from base64 import b64encode
import httplib


class MSKSercureWSParser(object):
	JSONList = None
	dbObj = None
	logger = None

	def __init__(self):
		self.JSONList= list()

	def getJSONList(self):
		return self.JSONList
	def getTotalJSON(self):
		return len(self.JSONList)


	def getDMSDBHandle(self): # need to access to dms db
		propObj = Properties()
		logger = DMPLogger()
 		logger = logger.getCVRLogger("getDMSDBHandle")
 		logger.debug("Creating database connection")
		logger.debug( "DMSDB object %s - %s - %s-%s"%(propObj.getDMSDBUser(),propObj.getDMSDBServer(),propObj.getDMSDBName(),propObj.getDMSDBPass()))
    		try:

			self.dbObj = DMPdb(
                       		propObj.getDMSDBUser(),
                       		propObj.getDMSDBPass(),
                       		propObj.getDMSDBServer(),
                       		propObj.getDMSDBName()
                     			)
			#self.dbObj = DMPdb()
    		except Exception as e:
        			logger.error(e)
        			dbObj = None
        			raise("DMS db handle not created!")
    		return self.dbObj

 	def insert_into_db(self):
		logger = DMPLogger()
 		logger = logger.getCVRLogger("")
 		logger.debug("INSERT INTO dms_crdb_interface table")
 		dbObj = self.getDMSDBHandle()
 		dbObj.connect() #need to connect to the dms db
 		try:
 			SQLInsertIntoMskSecure='''INSERT INTO dms_crdb_interface(prtId,ptMrn,regId,regEntryStartTime,sexDscrp,raceDscrp,
 							ethnicity,survivalStatusDscrp,survivalDate,
 							regIdDscrp,registrationDatetime,consentVersion,
 							orgnlConsentStatus,crntConsenStatus,
 							crntConsentDate,ptCrtDt,ptUpdDt,ptRegCrtDt,ptRegUpdDt)
						VALUES %r'''
			SQLSelect = '''SELECT * FROM dms_crdb_interface'''
			dbObj.execute(SQLSelect)
			rec = dbObj.fetchall()
			#logger.debug(rec)
			logger.debug(SQLInsertIntoMskSecure)
			key = 'list'
			self.getJSONList() # need to parseAPI first and retrieve them into json list
			#logger.debug(self.getJSONList())
			for ele in self.getJSONList():
				values =(ele.get('prtId'),ele.get('ptMrn'),ele.get('regId'),ele.get('regEntryStartTime'),ele.get('sexDscrp'),ele.get('raceDscrp'),ele.get('ethnicity'),ele.get('survivalStatusDscrp'),ele.get('survivalDate'),\
					ele.get('regIdDscrp'),ele.get('registrationDatetime'),ele.get('consentVersion'),ele.get('orgnlConsentStatus'),ele.get('crntConsenStatus'),ele.get('crntConsentDate'),ele.get('ptCrtDt'),\
					ele.get('ptUpdDt'),ele.get('ptRegCrtDt'),ele.get('ptRegUpdDt'))
				logger.debug('values')
				logger.debug(values)
				logger.debug(SQLInsertIntoMskSecure%(values,))
				logger.debug('self.dbObj%s'%dbObj)
				#SQLInsertIntoMskSecure%(values,)
				ret =dbObj.execute(SQLInsertIntoMskSecure%(values,))
				logger.debug('ret%s'%ret)
		except Exception as e:
 			logger.error(e)
 			dbObj = None
 			raise("insert into db failed")
		return True

 		#return self.dbObj

 		#query alys2sample table to get the alys2sample_id and ins

	def parseWS(self,url,userAndPass,usr,pwd,key):
		userAndPass=userAndPass.decode("ascii")
		headers = { 'Authorization' : 'Basic %s' %  userAndPass }
		#r = c.request('GET', '/', auth=HTTPDigestAuth('dmpuser','bPEi8HJzoH'),headers=headers)
		c=requests.get(url,auth=HTTPDigestAuth(usr,pwd),headers=headers)
		data = json.loads(c.text)
		listContent = data.get(key) #contentlist
		#self.JSONList = listContent
		for eledic in listContent:
			encodeEleDic  = dict((k.encode('ascii'), str(v).encode('ascii') if v else ' ') for (k, v) in eledic.items())
			#print encodeEleDic
			self.JSONList.append(encodeEleDic)

if __name__ == '__main__':
	parser = MSKSercureWSParser()
	url = 'http://plcrdba3:8787/rest-service/dmp/reg'
	userAndPass = 'ZG1wdXNlcjpiUEVpOEhKem9I'
	usr = 'dmpuser'
	pwd = 'bPEi8HJzoH'
	key = 'list'
	parser.parseWS(url,userAndPass,usr,pwd,key)
	parser.getJSONList(), parser.getTotalJSON()
	#print parser.getJSONList()[0].get('regEntryStartTime')
	#parser.getDMSDBHandle(),
	parser.insert_into_db()
	#print JSONList
