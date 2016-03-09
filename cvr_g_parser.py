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
from mskcc.dmp.cvr.utils.dmp_cvr_constants import DMPCvrConstants
from vcf_p import csv_reader2list


CVR_ANN_HEADER = []
CVR_G_ANN_HEADER = []
DEL_IN_CVR_G = []
ADD_IN_CVR_G = []


def head_strip2list(reader_list):
	annotated_col = reader_list[0].strip('\n').split('\t')
	return annotated_col

def getCVRANNH(location):
	cvr_ann_f =DMPCvrConstants.DMP_CVR_EXONIC_VARIANTS_FILE_NAME
	if os.path.exists(location + cvr_ann_f):
		cvr_ann_rdlist =  csv_reader2list(location + cvr_ann_f ,False)
		#print cvr_ann_rdlist[0].split('\t')
		CVR_ANN_HEADER.extend( head_strip2list(cvr_ann_rdlist )[:36])

	#return CVR_ANN_HEADER

def fetch_head(location):

	g_silent_f = DMPCvrConstants.DMP_CVR_SILENT_VARIANTS_FILE_NAME
	g_ann_f = DMPCvrConstants.DMP_CVR_EXONIC_VARIANTS_FILE_NAME
	g_np_silent_f = DMPCvrConstants.DMP_CVR_NP_SILENT_VARIANTS_FILE_NAME
	g_exon_covg_f = DMPCvrConstants.DMP_SAMPLE_COVG_FILE_NAME
	g_ss_f = DMPCvrConstants.DMP_SAMPLE_SHEET

	g_silent_rdlist=csv_reader2list(location+g_silent_f ,False)
	g_ann_rdlist = csv_reader2list(location+g_ann_f,False)
	g_np_silent_rdlist = csv_reader2list(location+g_np_silent_f ,False)
	g_exon_covg_rdlist = csv_reader2list(location+g_exon_covg_f,False)
	g_ss_f = csv_reader2list(location + g_ss_f,False)
	#print  location+g_silent_f #g_silent_hd
	#print g_ann_rdlist[0] , g_silent_rdlist[0],g_np_silent_rdlist[0],g_exon_covg_rdlist[0],g_ss_f[0]
	#print head_strip2list(g_ann_rdlist)[:40]
	CVR_G_ANN_HEADER.extend( head_strip2list(g_ann_rdlist)[:45])
	#print g_silent_rdlist[0] == g_ann_rdlist[0]
def delta_list (l1,l2):
	return list(set(l1) - set(l2))
def diff_CVR_vs_G():
	DEL_IN_CVR_G.extend(delta_list(CVR_ANN_HEADER,CVR_G_ANN_HEADER))
	ADD_IN_CVR_G.extend(delta_list(CVR_G_ANN_HEADER, CVR_ANN_HEADER))

if __name__ == '__main__':
	location = '/dmp/hot/prasadm/Pipeline/VAL/PathTest/'
	cvr_location = '/dmp/dms/qc-data/IMPACT/2015/IMPACTv5-CLIN-20150028/20150210_04.27/'
	getCVRANNH(cvr_location)
	print CVR_ANN_HEADER
	fetch_head(location)
	print CVR_G_ANN_HEADER
	diff_CVR_vs_G()
	print DEL_IN_CVR_G , len(DEL_IN_CVR_G), ADD_IN_CVR_G, len(ADD_IN_CVR_G)
	#print diff_cvrVSg(CVR_ANN_HEADER,CVR_G_ANN_HEADER)


