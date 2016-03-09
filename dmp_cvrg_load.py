'''
 Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.

 This script is free software; you can redistribute it and/or modify it
 under the terms of the GNU Lesser General Public License as published
 by the Free Software Foundation; either version 2.1 of the License, or
 any later version.

 This script is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF
 MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.  The software and
 documentation provided hereunder is on an "as is" basis, and
 Memorial Sloan-Kettering Cancer Center has no obligations to provide
 maintenance, support, updates, enhancements or modifications.  In no
 event shall Memorial Sloan-Kettering Cancer Center be liable to any
 party for direct, indirect, special, incidental or consequential damages,
 including lost profits, arising out of the use of this software and its
 documentation, even if Memorial Sloan-Kettering Cancer Center has been
 advised of the possibility of such damage.  See the GNU Lesser General
 Public License for more details.

 You should have received a copy of the GNU Lesser General Public License
 along with this library; if not, write to the Free Software Foundation,
 Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.

Created on April 27, 2015

@author: prasadm,wangy6

'''

from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from optparse import OptionParser
import sys, os
import subprocess
from mskcc.dmp.cvr.dao.dmp_alys_manger import DMPAlysManager
from mskcc.dmp.cvr.dao.dmp_germline_manager import GermlineSampleManager,IMPACTGermlineAnalysis
from mskcc.dmp.cvr.dao.dmp_alys_dir import IMPACTVCGAnalysisDir
from mskcc.dmp.cvr.utils.generic_utility import (getSysArchitecture, getUserName, getHostName )
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.generic_utility  import sendEmail, getYear
from dmp_germline_email import sendGLoadFailedEmail



def printVersion():
    print "Germline Clinical Variants Results Loader/API\n"
    print "Version 1.0\n"


'''
def sendGLoadFailedEmail(sample,logger,a2s_id=None,projectName=None):
    #logger.debug("Sending DMS-IMPACT-Germline failed alert e-mail to the subscribed attendees")
    logger.debug("Sending DMS-IMPACT-Germline failed alert e-mail to the subscribed attendees")
    try:
        #eMailList = 'prasadm@mskcc.org,wangy6@mskcc.org,syeda1@mskcc.org'
        eMailList ='wangy6@mskcc.org'
        subject = "[CVR-GERMLINE-FAILED-Alert] %s LOADING FAILED"%(sample)
        #htmlBody = "<br><br>Germline-Analysis/Annotation Failed for sample %s<br><br>"%(sample)
        sendEmail(subject, "Failed to load this Sample %s AT alys2sample_id  %s AT Project %s "%(sample,a2s_id,projectName), logger,eMailList)
    except Exception as e:
        logger.error("Error occurred while sending the DMS/CVRG alert e-mail for %s "%( sample))
        return False
    return True
'''



def main(logger, overwrite, SampleName, alysGResultsDir, snvOnly=True):
    #Germline Analysis Directory Object
    alysGDirObj = None

    #Create IMPACTGermlineAnalysis Object, which has the germline Manager to handle all sample mapping work, GermlineLoader object handles variants loading
    GermlineLoader = None

    if not os.path.isdir(alysGResultsDir):
        logger.error("The germline analysis directory provided is not a directory, nothing will be loaded. Dir: %s"%(alysGResultsDir))
        #GermlineLoader.sendCVRGLoadFailedEmail()
        sendGLoadFailedEmail(SampleName,logger)
        return False

    # Now create germline analysis directory object
    if SampleName:
        try:
            alysGDirObj = IMPACTVCGAnalysisDir(alysGResultsDir,SampleName)
            logger.debug('alysGDirObj.__dict__ %s' %alysGDirObj.checkDirExists())
        #### TBD ####
        # Validate the analysis directory object
        # alysDirObj.validate() # could throw an exception
        except Exception as e:
            logger.error("An exception occurred while creating the IMPACT Germline Analysis directory object..Dir: %s"%(alysGResultsDir))
            #GermlineLoader.sendCVRGLoadFailedEmail()
            sendGLoadFailedEmail(SampleName,logger)
            logger.error(e)
            return False

        try:
            GermlineLoader = IMPACTGermlineAnalysis(SampleName)
            if alysGDirObj:
                GermlineLoader.setAlysGDirObj(alysGDirObj) #loader set alys dir first
        except Exception as e:
            print SampleName,alysGDirObj
            logger.error("An exception occurred while adding Germline Analysis directory to the Germline Analysis Loader.")
            logger.error(e)
            print GermlineLoader.getErrorString()
            GermlineLoader.sendCVRGLoadFailedEmail()
            return False

        try:
            GermlineLoader.retrieve()
        except Exception as e:
            GermlineLoader.sendCVRGLoadFailedEmail()
            logger.error("An exception occurred while retrieving Germline analysis data for the Germline analysis manger.")
            logger.error(e)
            print GermlineLoader.getErrorString()
            return False

        try:
            GermlineLoader.sync()
        except Exception as e :
            logger.critical("An exception occurred while sync Germline analysis data for the Germline analysis manger.")
            logger.error(e)
            print GermlineLoader.getErrorString()
            GermlineLoader.sendCVRGLoadFailedEmail()
            return False

    logger.debug("Successfully completed loading the Germline Analysis Directory into the CVR-Germline database")
    return True


if __name__=="__main__":

    parser = OptionParser(version=printVersion())
    parser.set_description("This CLI is used to load the variant results from the IMPACT Germline pipeline into the CVR Germline database. Please be very careful in setting up the PYTHONPATH and database connectivity.")
    #parser.add_option("-a", "--analysis-name", dest="alysName", help="[REQUIRED] Analysis name for already existing or completed Analysis from DMS", default=None)
    parser.add_option("-o", "--overwrite-results", dest="overwrite", help="Overwrite the analysis results for analysis. Please use this ONLY when you know what is happening with the backend.", action="store_true", default=False)
    #parser.add_option("-i", "--identifier-analysis", dest="alysId", help="Analysis task identifier, essentially from the DMS but also stored in variants database", default=False)
    parser.add_option("-r","--results-dir", dest="alysGResultsDir", help="[REQUIRED] This directory contains analysis results/variants from IMPACT Germline pipeline", default=None)
    parser.add_option("-u","--user-loading", dest="personId", help="Analyst how is loading these results into the database", default=None)
    parser.add_option("-v", "--version", dest="version", help="Print the version Id for the CVR Germline loader and API", action="store_true", default=False)
    #parser.add_option("-c","--cnv-load", dest="cnvOnly", help="Load only copy number variants into the database, assuming SNPs are already loaded.", action="store_true", default=False)
    parser.add_option("-s", "--sampleName", dest="dmp_assay_lbl", help="[REQUIRED] DMP ID AKA dmp_assay_lbl in alys2sample table", default=None)
    parser.add_option("-p", "--project", dest="project", help="project name", default=None)
    #parser.add_option("-p", "--purge-results", dest="purgeAnalysis", help="[NOT FUNCTIONAL]Purge the variants from the CVR database, this should be done as adminstrator of the CVR. Please be very careful using this option", default=None)

    (opts, args) = parser.parse_args()
    #
    ## get only version
    if opts.version: sys.exit(0)

    #mAlysName = opts.alysName
    mOverwrite = opts.overwrite
    #mAlysTaskId = opts.alysId
    mAlysGResultsDir = opts.alysGResultsDir
    mAnalystId = opts.personId
    #mCnvOnly = opts.cnvOnly
    mSampleName = opts.dmp_assay_lbl
    #mPurge = opts.purgeAnalysis
    mProject = opts.project

    if mAlysGResultsDir == None :
        print ("IMPACT Germline analysis results directory is required for loading the variants into the database.\n\n")
        print "For usage call the CVR-G loader with -h/--help argument.\n"
        sys.exit(1)

    mLogger = DMPLogger()
    mLogger = mLogger.getCVRLogger("dmp_cvr_germline_load")
    mLogger.debug("Invoking CVR_G loader...")
    mLogger.debug("User: %s \nHostname: %s \nSystemArchitecture: %s"%(getUserName(), getHostName(), getSysArchitecture()))

    #
    # check if this is production machine and if production user is loading this
    prop = Properties()
    if getHostName() == prop.getProdHostName():

        if getUserName() != prop.getProdUser():
            print "ERROR: You can't load the variants from a PRODUCTION server, only production user is allowed from this host-name."
            print "Sorry! no cookie for you!"
            ################
            ##### TODO #####
            # Implement sending an e-mail alert to the Admins
            sys.exit(1)

    if mOverwrite :
        mLogger.debug("You have chosen to overwrite the CVR_G variants from previous analysis (if any). Be cautious of this action.")

    #only when annotation files have been loaded successfully,
    #call vcf_loader, ./vcf_p.py -f /dmp/hot/prasadm/Test/IMPACTv5-VAL-20140177/germline/annotation/P-0002413-N01.annotated.vcf
    if main(mLogger, mOverwrite, mSampleName, mAlysGResultsDir):
        print 'MSK Germline Variants Annotated files have been loaded up successfully !'
        try:
            #mSampleName = mSampleName+'-IM5' if not mSampleName.endswith('-IM5') else mSampleName
            vcf = mAlysGResultsDir+'/'+mSampleName+'.annotated.vcf'
            subprocess.check_output("./vcf_p.py -f %s "%(vcf),shell=True)
            sys.stdout.write("VCF %s has been loaded Successfully!\n" %vcf)
            #subprocess.check_output("python dmp_germline_email.py -s %s "%(mSampleName),shell=True)
        except Exception as e :
            mLogger.critical("Loading VCF failed.")
            mLogger.error(e)
            sys.stdout.write("VCF %s loading failed!\n" %vcf)
        try:
            subprocess.check_output("/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python  dmp_germline_email.py -s %s "%(mSampleName),shell=True)
        except Exception as e:
            mLogger.critical("Sending final email notification failed.")
            mLogger.error(e)
            sys.stdout.write("dmp_germline_email.py -s %s failed!\n" %mSampleName)
        sys.exit(0)
    else:
        sys.stdout.write("MSK Germline Variants Annotated files loading failed !")
        #subprocess.check_output("/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python  dmp_germline_email.py -s %s "%(mSampleName),shell=True)
        sys.exit(1)


    #sys.exit(0) if main(mLogger, mOverwrite, mSampleName, mAlysGResultsDir) else sys.exit(1)




