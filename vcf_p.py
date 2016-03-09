#!/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python

# -*- coding: utf-8 -*-
"""
@author: wangy6@mskcc.org
"""
import os
import sys
import csv
import argparse
import re
import subprocess
from os.path import basename
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb


#h_pref = '##' ; HEADER = ['INFO','FORMAT']
INFOR_FIELDS = ['TRANSCRIPT_ID','HGVS_TRANSCRIPT','HGVS_PROTEIN',
				'TRANSLATION_IMPACT','ING_CLASSIFICATION',
				'ING_PHENOTYPE','ING_AFFECTED',
				'ING_AFFECTED_HOM','ING_UNAFFECTED']
FORMAT_FIELDS  =['ING_CH','ING_IA']
META_HEADER = ['##source']

INFO_H= '##INFO' ; ING_ID = 'ING_' ; KB_V = 'Version'
FMT_H = '##FORMAT' ; ID = 'ID' ; Description = 'Description'

META_LIST = []
ING_MAP=[]
INFO= [] #stpre each row's INFO fileds value list into BIG list
FMT = []#stpre each row's  FORMATS fileds mapping dict  into BIG list
workfl_head = [['vendor_name','workflow_version','kbase_version']]
EXT_ANN_WFL_ID = 1

dmp_assay_lbl =None #sample_id in vcf file, paired with format field
#alys2sample_id = None #alys2sample_id in the alys2sample table
COMPOUND_KEY = ['CHROM','POS','REF','ALT','TRANSCRIPT_ID'] # TRANSCRIPT_ID in VCF need to idenify the
KEY_RS = []
SNP_INDLKEY_MAP = {}
F_Prefix = ''
HEAD_FILE = '_h.vcf.tmp' ; CONTENT_FILE = '_content.vcf.tmp' ; INFO_FILE = '_info.vcf.tmp' ; FMT_FILE = '_format.vcf.tmp'

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
        raise ("Database handle not created!")
    return dbObj

def getDMSDBHandle(logger): # need to fetch ingenuity_url  from ingenuity_logs table in dms_db
    dbObj = None
    propObj = Properties()
    logger.debug( "DMSDB object %s - %s - %s"%(propObj.getDMSDBUser(),propObj.getDMSDBServer(),propObj.getDMSDBName()))
    try:
        dbObj = DMPdb(
                       propObj.getDMSDBUser(),
                       propObj.getDMSDBPass(),
                       propObj.getDMSDBServer(),
                       propObj.getDMSDBName()
                     )
    except Exception as e:
        logger.error(e)
        dbObj = None
        raise("DMS db handle not created!")
    return dbObj
'''
    Given dbObj cursor,and sql statement, execute the sql query results
'''
def sql_cursor_exe(dbObj,sql):
    dbObj.execute(sql) #bool
    rec = dbObj.fetchall() #tuple with each dict element
    # print the column names
    if rec:
        print_formatted_columns(rec[0].keys())
        for r in rec:
            print_formatted_columns(r.values())
    else:
        pass



def Str2Dic(s): # convert 'a=b' to {a:b}
	# get all the items
	matches = re.findall(r'\w+=".+?"', s) + re.findall(r'\w+=[\d.]+',s)
	# partition each match at '='
	matches = [m.group().split('=', 1) for m in matches]
	d = dict(matches)
	return d


def head_reader(h_file):
	h_list =csv_reader2list(h_file,False)	#need to store the f_reader into list
	kb_v = ''
	for h in META_HEADER:
		for r in h_list:
			if r.startswith(h):
				#print 'META_HEADER: %s' % r.strip(h+'=')
				src = r.strip(h+'=\n').split(' ',1) #split the first occurance
				META_LIST.extend(src)
			elif r.startswith(INFO_H) and r.find(ING_ID)!=-1:
				if r.find(KB_V) != -1:
					#print re.sub('[^0-9a-zA-Z\.]+', '', r[ r.find(KB_V): ].strip(KB_V))
					kb_v=re.sub('[^0-9a-zA-Z\.\+]', '',r[r.find(KB_V):].strip(KB_V) )
					#kb_v = r.split('=')[1]
			elif r.startswith(INFO_H) or r.startswith(FMT_H):
					r= r[r.find(ID):].strip('>\n')
					r_list = r.split(',')
					#ING_MAP =[[],[],[]...]
					ING_MAP.append( [r[r.find('='):].strip('"=') for r in r_list if r.startswith(ID) or r.startswith(Description) ] )# only fetch ID: Description
					#ING_MAP

	META_LIST.append(kb_v)
	#print ING_MAP
	return META_LIST




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

def extract_INFO(info_f):
	rs = csv_reader2list(info_f)
	column_name = rs[0]
	for e in rs[1:]:	#read records
		e_list= e[0].split(';')#[' ...;...; '] only one ele in the list
		temp = [ '' for i in range(len(INFOR_FIELDS) ) ] # map the order in the INFOR_FIELDS
		for INF in INFOR_FIELDS:
			for e_str in e_list:
				if e_str.startswith(INF) and e_str.split('=')[0] ==INF : # must double check cuz ING_AFFECTED and ING_AFFECTED_HOM confusion
					#print e_str, INFOR_FIELDS.index(INF)
					temp[ INFOR_FIELDS.index(INF) ] = e_str.split('=')[1].strip('"')#clean up records
		#print temp #,len(temp)
		INFO.append(temp)

	return INFO

def extract_FMT(fmt_f):
	rs = csv_reader2list(fmt_f)
	column_name  = rs[0]
	global  dmp_assay_lbl #de-identified sample_id in vcf,
	dmp_assay_lbl =  column_name[1]
	dmp_assay_lbl = dmp_assay_lbl+'-IM5' if not dmp_assay_lbl .endswith('-IM5') else dmp_assay_lbl
	for indx, e in enumerate(rs[1:]):
		#print indx
		k =e[0].split(':') ; v =  e[1].split(':')
		dic = dict(zip(k,v))
		fmt_dic= dict( filter(lambda (k,v): k in FORMAT_FIELDS, dic.items()) )
		fmt_klist = fmt_dic.keys()
		fmt_vlist = fmt_dic.values()
		#add sample_id in vcf to format_value_list
		#fmt_vlist .append(dmp_assay_lbl )
		#print fmt_klist ,fmt_vlist
		FMT.append(fmt_vlist)
		INFO[indx].extend( fmt_vlist)
	return FMT

def extract_Compound_Keys(content_file):
	rs = csv_reader2list(content_file)
	column_name = rs[0]
	for indx,e in enumerate(rs[1:]): # map each line with TRANSCRIPT_ID, need to strip out .
		transcript_id =INFO[indx][0].split('.')[0] #1st element is TRANSCRIPT_ID . NM_007294.3
		KEY_RS.append([int(e[0].replace('chr','')), int(e[1]),e[3],e[4],transcript_id])
	return KEY_RS


def add_ids(ext_annotation_workflow_id, dmp_assay_lbl , alys2sample_id): # add, ext_annotation_workflow_id,dmp_assay_lbl and alys2sample_id
	for info in INFO:
		#info.append(EXT_ANN_WFL_ID)
		info.extend( (ext_annotation_workflow_id, dmp_assay_lbl , alys2sample_id) )


def _write_file(f,data,header_l_list=None): #write to file
    with open(f, 'wb') as fp:
        a = csv.writer(fp,delimiter='\t')
        #a.writerows([['tissue_type','tumor_type','code'],])
        if header_l_list:
        	a.writerows(header_l_list)
        a.writerows(data)

def split_file(f): # split the original vcf into header,info and fmt sun files for further parsing work
	content_line_indx = 0
	if os.path.exists(f):
		fp = open(f,'rU')
		for i,line in enumerate(fp):
			if not line.startswith('##'): #find the line index that is not header meta-data
				content_line_indx = i
				break
		global F_Prefix
		F_Prefix = os.path.splitext(basename(f))[0]
		subprocess.check_output('head -n %s %s>%s'%(content_line_indx,f,F_Prefix+HEAD_FILE),shell=True)
		subprocess.check_output("sed -n '%s,$p'  %s>%s"%(content_line_indx+1,f,F_Prefix+CONTENT_FILE),shell=True)
	if os.path.exists(F_Prefix+CONTENT_FILE):
		subprocess.check_output("cut -f 8 -d$'\t' %s > %s" %(F_Prefix+CONTENT_FILE,F_Prefix+INFO_FILE),shell=True)
		subprocess.check_output("cut -f 9,10 -d$'\t' %s > %s"%(F_Prefix+CONTENT_FILE,F_Prefix+FMT_FILE),shell=True)

def insert_into_db():
	logger = DMPLogger()
 	logger = logger.getCVRLogger("GermLineDB")
 	logger.debug("Creating database connection")
	dbObj = None

	try:
		dbObj = getCVRDBHandle(logger)
 		dbObj.connect()
 		logger.debug("Successful connection")
 		#query alys2sample table to get the alys2sample_id and insert into the germline_annotation table
 		SQLGetA2SID = '''SELECT alys2sample_id,lims_sample_id  from alys2sample a2s JOIN dmp_sample s
 				ON a2s.dmp_sample_id = s.dmp_sample_id  WHERE dmp_assay_lbl = '%s'
 				AND sample_type_cv_id =1'''%dmp_assay_lbl # hard coded task
 		logger.debug(SQLGetA2SID)
 		try:
 			retA2Ssql = dbObj.execute(SQLGetA2SID)
 			if retA2Ssql:
 				sqlRet = dbObj.fetchnext() #return dict
 				logger.debug(sqlRet)
 				alys2sample_id= int ( sqlRet.get('alys2sample_id') )#dict {'alys2sample_id': 4715L}
 				lims_sample_id = sqlRet.get('lims_sample_id')
 		except Exception as e:
			logger.error("An exception occurred while fetching the alys2sample_id for dmp_assay_lbl %s"%dmp_assay_lbl)
			logger.error(e)
			return False
		logger.debug('SQLGetA2SID%s,%s'%(alys2sample_id,lims_sample_id))


		try:
			ingenuity_infor = get_ing_info(lims_sample_id,dmp_assay_lbl)
			logger.debug(ingenuity_infor)
			SQLInsertURL = '''INSERT INTO dms_ingenuity_info(lims_sample_id,dmp_assay_lbl,dmg_number,ingenuity_url ) VALUES %r'''
			logger.debug(ingenuity_infor)
			dbObj.execute(SQLInsertURL%(ingenuity_infor,))
		except Exception as e:
			logger.error("An exception occurred while fetching dms_ingenuity_info for dmp_assay_lbl %s"%dmp_assay_lbl)
			logger.error(e)
			return False

		try:
			mt = tuple(META_LIST)
			#logger.debug('tuple is %s ' %ml)
			#logger.debug(SQLInsertIntoWFL %(mt,) )
			#logger.debug( SQLGetWFLID)
			SQLInsertIntoWFL = '''INSERT INTO ext_annotation_workflow(vendor_name,workflow_version,kbase_version)VALUES %r'''
			dbObj.execute(SQLInsertIntoWFL %(mt,) )
			SQLGetWFLID ="SELECT ext_annotation_workflow_id FROM ext_annotation_workflow WHERE vendor_name ='%s'AND workflow_version='%s' AND kbase_version='%s' "%(mt[0],mt [1],mt[2])
			dbObj.execute(SQLGetWFLID)
			ext_annotation_workflow_id = int(dbObj.fetchnext()['ext_annotation_workflow_id'])
			logger.debug('ext_annotation_workflow_id%s' %ext_annotation_workflow_id )
		except Exception as e:
			logger.error("An exception occurred while inserting records into ext_annotation_workflow for dmp_assay_lbl %s"%dmp_assay_lbl)
			logger.error(e)
			return False

		#get snp_indel_id mapping
		extract_Compound_Keys(F_Prefix+CONTENT_FILE)
 		SQLGetSnpIndID = '''SELECT snp_indel_variant_id  FROM dmp_variants dv JOIN
 			snp_indel_variants snp ON snp.dmp_variant_id =dv.dmp_variant_id
 			WHERE chromosome = %s AND start_position = %s  AND ref_allele = '%s' AND alt_allele ='%s' AND snp.alys2sample_id = %s '''
 		for index, ck in enumerate(KEY_RS):
 			logger.debug(SQLGetSnpIndID%(ck[0],ck[1],ck[2],ck[3],alys2sample_id))
 			try:
 				retSnpIdSQL = dbObj.execute(SQLGetSnpIndID%(ck[0],ck[1],ck[2],ck[3],alys2sample_id))
 				if retSnpIdSQL:
 					snp_id = int( dbObj.fetchnext()['snp_indel_variant_id']  )
 				logger.debug('snp_id%s'%snp_id)
 				INFO[index].append(snp_id) #append the snp_id to the  germline_annotation insertion list
 			except Exception as e:
				logger.error("An exception occurred while fetching the snp_indel_id for dmp_assay_lbl %s"%dmp_assay_lbl)
				logger.error(e)
				return False
				#snp_id = ''

 		#append all the required id into the INFO list
		add_ids(ext_annotation_workflow_id,dmp_assay_lbl ,alys2sample_id)
		#logger.debug('INFOLIST%s'%INFO)

		for inf_l in INFO:
			inft = tuple(inf_l)
			try:
				SQLInsertIntoGA ='''INSERT INTO germline_annotation(
 								transcript_id,hgvs_cDNA_annotation,hgvs_aa_annotation,
 								translation_impact,assesment,ing_phenotype,
 								ing_affected_count,ing_affected_hom_count,ing_unaffected_count,
 								inferred_activity,inferred_compound_heterozygous,snp_indel_variant_id,
 								ext_annotation_workflow_id,dmp_assay_lbl,alys2sample_id)VALUES
								%r'''
				logger.debug(SQLInsertIntoGA %(inft,))
				dbObj.execute(SQLInsertIntoGA%(inft,))
			except Exception as e:
				logger.error("An exception occurred while inserting INFOR_FIELDS into germline annotation for dmp_assay_lbl %s"%dmp_assay_lbl)
				logger.error(e)
				return False

		for  ing_ml in ING_MAP:
			ingt = tuple(ing_ml)
			try :
				SQLInsertIntoINGDMMAP='''INSERT INTO ing_dm_mapping(
							ing_dm_name,misc)VALUES %r'''
				logger.debug(SQLInsertIntoINGDMMAP%(ingt,))
				dbObj.execute(SQLInsertIntoINGDMMAP%(ingt,))
			except Exception as e:
				logger.error("An exception occurred while inserting mapping_info into ing_dm_mapping for dmp_assay_lbl %s"%dmp_assay_lbl)
				logger.error(e)
				return False
		#dbObj.execute(SQLInsertIntoWFL %(mt,) )
	except Exception as e :
		logger.error(e)
		logger.error("An exception occurred while inserting vcf records for dmp_assay_lbl %s"%dmp_assay_lbl)
		return False
	finally:
		if dbObj:
			dbObj.disconnect()

	logger.debug('Done loading VCF into germline_annotation table ')
	return True

def get_ing_info(lims_sample_id,dmp_assay_lbl): #upload the ingenuity_url from dms ingenuity_logs table by giving a sample_lbl
	logger = DMPLogger()
 	logger = logger.getCVRLogger("GermLineDB connect DMS DB")
 	logger.debug("Creating dmp_dms database connection")
	dbObj = None
	ing_url = None
	dmg_number = None
	try:
		dbObj = getDMSDBHandle(logger)
 		dbObj.connect()
 		logger.debug("Successful connection to dms_db")
 		SQLGetURL = '''SELECT * FROM  ingenuity_logs WHERE sample_name ='%s' '''%dmp_assay_lbl
 		SQLGetDMG_NUM ='''SELECT * FROM dmp_gl_annotation WHERE  sample_name='%s' '''%lims_sample_id
 		logger.debug(SQLGetURL),logger.debug(SQLGetDMG_NUM)
 		logger.debug(dbObj.execute(SQLGetURL)) , logger.debug(dbObj.execute(SQLGetDMG_NUM))
 		if dbObj.execute(SQLGetURL):
 			ing_url = dbObj.fetchnext()['ingenuity_url'] #{'ingenuity_url': 'https://api.ingenuity.com/datastream/analysisStatus.jsp?packageId=DP_104160784239026244548'}
 		logger.debug('ing_url %s' %ing_url)

 		if dbObj.execute(SQLGetDMG_NUM):
 			dmg_number  = dbObj.fetchnext()['dmg_number']
 		logger.debug('dmg_number%s'%dmg_number)
 	except Exception as e :
		logger.error(e)
		logger.error("An exception occurred while inserting ingenuity_infor records for dmp_assay_lbl %s, lims_sample_id %s" %(dmp_assay_lbl,lims_sample_id ) )
	finally:
		if dbObj:
			dbObj.disconnect()
	return (lims_sample_id,dmp_assay_lbl,dmg_number,ing_url)

def main(): # Setting up argparse,accepting user input arguments to generate command line
	parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")

	parser.add_argument('-f','--file_name',action='store', dest='file',
                    required=True, help='***LOAD INTO GermLineDB*** \n'+
                                'Please give the file name that needs to be loaded\n')

    	user_args = parser.parse_args()
    	f= user_args.file

	split_file(f)

	head_reader(F_Prefix+HEAD_FILE)
	#print ING_MAP
	#print META_LIST

	extract_INFO(F_Prefix+INFO_FILE)
	extract_FMT(F_Prefix+FMT_FILE)
	#add_ids()
	print 'VCF loading for Sample(dmp_assay_lbl) %s ... '%dmp_assay_lbl
	if insert_into_db():
	#for i in range( len(INFO) ):
	#	print INFO[i] ,len(INFO[i])
		print 'Total %s new loading %s records from VCF ' %(len(KEY_RS),len(INFO))
		#remove splitted tmp files

		os.remove(F_Prefix+HEAD_FILE)
		os.remove(F_Prefix+CONTENT_FILE)
		os.remove(F_Prefix+INFO_FILE)
		os.remove(F_Prefix+FMT_FILE)

		sys.stdout.write("VCF %s has been loaded Successfully!\n" %f)
	else:
		sys.stdout.write("VCF %s loading failed !\n" %f)
		sys.exit(1)


if __name__ == '__main__':
	main()
	#get_ing_info('P-0003778-N01-IM5')


