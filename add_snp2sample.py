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

class AddSnp2Sample(object):
	dbObj = None
	alys2sample_id = None
	dmp_variant_id = None
	snp_indel_variant_id = None 
	variant_class_set = None
	variant_class_map = None
	dmp_variants = None
	dmp_variants_meta = None 
	chromosome = None
	gene_id = None
	exon_num = None
	start_position = None
	ref_allele = None
	alt_allele = None
	transcript_id = None
	snp_indel_variants = None



	def __init__(self,alys2sample_id,dmp_variants,snp_indel_variants):
		self.alys2sample_id  = alys2sample_id
		self.dbObj = self.getCVRDBHandle()
		self.variant_class_set= ['nonsynonymous_SNV','synonymous_SNV','frameshift_deletion','frameshift_insertion','stopgain_SNV','stoploss_SNV',\
					'exonic','nonframeshift_deletion','nonframeshift_insertion','splicing','UTR5','UTR3','promoter','upstream','intronic','downstream',\
					'intergenic','Unknown','splicing_noncanonical']
		self.variant_class_map = dict(zip(self.variant_class_set,range(1,len(self.variant_class_set)+1)))
		self.dmp_variants = dmp_variants
		self.dmp_variants_meta = tuple()
		self.snp_indel_variants = snp_indel_variants

	def getCVRDBHandle(self):
		propObj = Properties()
		logger = DMPLogger()
		dbObj = None
		logger = logger.getCVRLogger("getCVRDBHandle")
 		logger.debug("Creating database connection for Adding snp_indel for a sample ")
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

 	def getDBObject(self):
    		return self.dbObj

	def getVariantClassSet(self):
    		return self.variant_class_set

	def getVariantClassMap(self):
    		return self.variant_class_map

    		#pass
	def getA2SId(self):
		return self.alys2sample_id

	def getDmpVariantID(self):
		return self.dmp_variant_id

	def getSNPVariant(self):
		return self.snp_indel_variants

	def getSNPIndelID(self):
		return self.snp_indel_variant_id 

	def getVariantClassCVId(self,variant_class):
		if self.variant_class_map.get(variant_class):
			return self.variant_class_map.get(variant_class)
		else:
			raise ValueError("Invalid variant_class %s  in table " % variant_class)

	def getVariantsMeta(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("getVariantsMeta")
		vlist = self.dmp_variants.split('/')
		#chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,cDNA_change,aa_change,variant_class,
		#dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted
		logger.debug('dmp_variants%s'%vlist[11])
		if len(vlist) != 17:
			raise ValueError('Invalid dmp_variants list set %s, total fields length is %s' %(self.dmp_variants,len(vlist)) )
		self.chromosome = vlist[0];self.gene_id=vlist[1];self.exon_num=vlist[2];self.start_position=vlist[3];self.ref_allele = vlist[5];self.alt_allele=vlist[6];self.transcript_id=vlist[8]
		logger.debug('%s-%s-%s-%s-%s-%s-%s'%(self.chromosome,self.gene_id,self.exon_num,self.start_position,self.ref_allele,self.alt_allele,self.transcript_id))
		variant_class = vlist[11]
		#logger.debug('variant_class%s'%variant_class)
		variant_class_id = self.getVariantClassCVId(variant_class)
		#logger.debug('variant_class_id%s'%variant_class_id)
		vlist[11] = variant_class_id
		vlist.extend([1, 1])
		self.dmp_variants_meta = tuple(vlist)
		return self.dmp_variants_meta

	def getSnpMeta(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("getVariantsMeta")
		vlist = self.dmp_variants.split('/')


	def InsertSnpMeta(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("getSnpMeta")
		logger.debug("INSERT snp_indel_variants")
		dbObj = self.dbObj
 		dbObj.connect() #need to connect to the cvr db
 		retVal = True
 		try:
 			SqlInsertIntoSNP='''INSERT INTO snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
			tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps,tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
			occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values%r'''
 		except Exception, e:
 			raise
 		else:
 			pass
 		finally:
 			pass





	def fetchDmpVariants(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("fetchDmpVariants")
		logger.debug("Check if dmp variants exist in table")
		dbObj = self.dbObj
 		dbObj.connect() #need to connect to the cvr db
 		retVal = True
 		try:
 			SqlGetDMPVariants='''SELECT * from dmp_variants WHERE
 				chromosome='%s' AND gene_id='%s' AND exon_num='%s' AND start_position=%s AND ref_allele ='%s' AND
 				alt_allele='%s' AND transcript_id ='%s' '''
 			logger.debug(SqlGetDMPVariants%(self.chromosome,self.gene_id,self.exon_num,\
 				self.start_position,self.ref_allele,self.alt_allele,self.transcript_id))
 			dbObj.execute(SqlGetDMPVariants%(self.chromosome,self.gene_id,self.exon_num,\
 				self.start_position,self.ref_allele,self.alt_allele,self.transcript_id))
			ret = dbObj.fetchone()
 			logger.debug(ret)
 			if ret:
 				retVal = True
 				self.dmp_variant_id = int( ret['dmp_variant_id'] )
 			else:
				SqlInsertIntoDMPVariants ='''INSERT INTO
 					dmp_variants(chromosome,gene_id,exon_num,start_position,stop_position,
 					ref_allele,alt_allele,is_indel,transcript_id,cDNA_change,aa_change,
 					variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,
 					is_hotspot,is_blacklisted,vdb_version_id,hit_counter)
 					VALUES %r'''
				logger.debug(SqlInsertIntoDMPVariants %(self.getVariantsMeta(),))
				try:
					retVal = dbObj.execute(SqlInsertIntoDMPVariants %(self.getVariantsMeta(),))
					if not retVal:
						raise ValueError('Error occured when inserting into db!')
					else:
						SQLgetDmpVariantID = '''SELECT MAX(dmp_variant_id) AS dmp_variant_id FROM dmp_variants'''
						dbObj.execute(SQLgetDmpVariantID)
						ret = dbObj.fetchone()
						if ret:
							retVal = True
							self.dmp_variant_id = int( ret['dmp_variant_id'] )
						else:
							raise ValueError('Error,cannnot fetch dmp_variant_id!')
				except Exception as e:
					logger.error(e)
 		except Exception as e:
 			logger.error(e)
 			dbObj = None
 			raise ValueError('Error fetch Dmp Variants for %s in DB'%self.getA2SId())
		return retVal


	def addSNP2Sample(self):
		logger = DMPLogger()
		logger = logger.getCVRLogger("add Snp Indel to Sample ")
		logger.debug("Check if dmp_alys2sample_id is in dmp_sample  table")
		dbObj = self.dbObj
 		dbObj.connect() #need to connect to the cvr db
 		try:

 			SqlInsertIntoSNP='''INSERT INTO snp_indel_variants(snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,
 				tumor_dp,tumor_rd,tumor_ad,tumor_vfreq,tumor_rd_ps,tumor_ad_ps,tumor_rd_ns,tumor_ad_ns,normal_agg_ad,
 				normal_vfreq_median,support_for_som_call,occurance_in_normal,occurance_in_pop,is_silent,is_panel,
	 			variant_status_cv_id,comments,confidence_cv_id,is_manually_loaded,alys2sample_id,dmp_variant_id)values %r'''
 			logger.debug(SqlInsertIntoSNP%(snp_indel_variants,))
 			dbObj.execute(SqlInsertIntoSNP%(snp_indel_variants,))
 			logger.debug(ret)
 			if ret:
 				retVal = True
 				#self.dmp_variant_id = int( ret['dmp_variant_id'] )
 			else:
 				retVal = False
 		except Exception as e:
 			logger.error(e)
 			dbObj = None
 			raise ValueError('Error addSNP2Sample %s in DB'%self.getA2SId())
		return retVal




def main():
	# Setting up argparse,accepting user input arguments to generate command line
	parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")

	parser.add_argument('-a','--alys2sample_id',action='store', dest='alys2sample_id',
                    required=False, help='***Checking dmp_sample object*** \n'+
                                'Please give the a2sid that needs to be loaded\n')
	parser.add_argument('-dv','--dmp_variants',action='store', dest='dmp_variants',
                    required=False, help='***Checking dmp_variants object*** \n'+
                                'Please give the dmp_variants meta data need to be loaded\n')
	parser.add_argument('-snp','--snp_indel_variants',action='store',dest='snp_indel_variants',
		required = False,help='***Checking snp_indel_variants object*** \n'+
                                'Please give the snp_indel_variants meta data need to be loaded\n')
	user_args = parser.parse_args()
	try :
		a2sid = user_args.alys2sample_id
	except:
		a2sid = ''
	try:
		dmp_variants = user_args.dmp_variants
	except:
		dmp_variants = ''

	try:
		snp_indel_variants = user_args.snp_indel_variants
	except:
		snp_indel_variants = ''

	#4,TET2,exon9,10619,'',C,A,1,NM_001127208,c.4075C>A,p.R1359S,nonsynonymous_SNV,rs112921115,'',0.0009,0,0'
	#
	addobj = AddSnp2Sample(a2sid,dmp_variants,snp_indel_variants)
	addobj.getCVRDBHandle()
	print addobj.getVariantClassSet(), addobj.getVariantClassMap(),addobj.getVariantClassCVId('splicing_noncanonical')
	print addobj.getVariantsMeta()
	print addobj.fetchDmpVariants(),addobj.dmp_variant_id
	#print addobj.getDmpVariantID()


if __name__ == '__main__':
	main()