'''
 Copyright (c) 2013 Memorial Sloan-Kettering Cancer Center.

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

Created on Jan 23, 2014

@author: syeda1

'''

from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from optparse import OptionParser
import sys, os
from mskcc.dmp.cvr.dao.dmp_alys_manger import DMPAlysManager
from mskcc.dmp.cvr.dao.dmp_alys_dir import IMPACTVCAnalysisDir
from mskcc.dmp.cvr.utils.generic_utility import (getSysArchitecture, getUserName, getHostName )
from mskcc.dmp.cvr.utils.dmp_properties import Properties


def printVersion():
    print "Clinical Variants Results Loader/API\n"
    print "Version 1.0\n"



def main(logger, overwrite, alysName, alysTaskId, alysResultsDir, cnvOnly=False, snvOnly=False, svOnly=False):
    #Analysis Directory Object
    alysDirObj = None

    # Create AnalysisTask Manager, although here only one analysis is being loaded [in future should be able to load multiple analysis]
    alysManager = DMPAlysManager(overwrite)
    if alysManager == None:
        logger.critical("Unable to create a Analysis Manger object, nothing could be done")
        return False

    if not os.path.isdir(alysResultsDir):
        logger.error("The analysis directory provided is not a directory, nothing will be loaded. Dir: %s"%(alysResultsDir))
        return False

    # Now create analysis directory object
    try:
        alysDirObj = IMPACTVCAnalysisDir(alysResultsDir)
        #### TBD ####
        # Validate the analysis directory object
        # alysDirObj.validate() # could throw an exception
    except Exception as e:
        logger.error("An exception occurred while creating the IMPACT Analysis directory object..Dir: %s"%(alysResultsDir))
        logger.error(e)
        return False

    if alysName != None:
        try:
            logger.debug('alysName %s,' %(alysName))
            logger.debug(' alysDirObj %s'%alysDirObj )
            alysManager.addAnalysisDir(alysName, alysDirObj)

        except Exception as e:
            print alysName,alysDirObj
            logger.error("An exception occurred while adding Analysis directory to the Analysis manager.")
            logger.error(e)
            print alysManager.getErrorString()
            return False
    elif alysTaskId != None:
        # Not implemented yet
        alysManager.addAnalysisById(alysTaskId)
        # Return since not implemented
        print "This functionality is not implemented, so returning with SUCCESS..."
        return True

    try:
        alysManager.retrieve()
    except Exception as e:
        logger.critical("An exception occurred while retrieving analysis data for the analysis manger.")
        logger.error(e)
        print alysManager.getErrorString()
        return False

    try:
        alysManager.sync()
    except Exception as e:
        logger.critical("An exception occurred while retrieving analysis data for the analysis manger.")
        logger.error(e)
        print alysManager.getErrorString()
        return False

    logger.debug("Successfully completed loading the Analysis Directory into the CVR database")
    return True



if __name__=="__main__":
    parser = OptionParser(version=printVersion())
    parser.set_description("This CLI is used to load the variant results from the IMPACT pipeline into the CVR database. Please be very careful in setting up the PYTHONPATH and database connectivity.")
    parser.add_option("-a", "--analysis-name", dest="alysName", help="[REQUIRED] Analysis name for already existing or completed Analysis from DMS", default=None)
    parser.add_option("-o", "--overwrite-results", dest="overwrite", help="Overwrite the analysis results for analysis. Please use this ONLY when you know what is happening with the backend.", action="store_true", default=False)
    parser.add_option("-i", "--identifier-analysis", dest="alysId", help="Analysis task identifier, essentially from the DMS but also stored in variants database", default=False)
    parser.add_option("-r","--results-dir", dest="alysResultsDir", help="[REQUIRED] This directory contains analysis results/variants from IMPACT pipeline", default=None)
    parser.add_option("-u","--user-loading", dest="personId", help="Analyst how is loading these results into the database", default=None)
    parser.add_option("-v", "--version", dest="version", help="Print the version Id for the CVR loader and API", action="store_true", default=False)
    parser.add_option("-c","--cnv-load", dest="cnvOnly", help="Load only copy number variants into the database, assuming SNPs are already loaded.", action="store_true", default=False)
    parser.add_option("-s", "--samplesheet", dest="ssFSLoc", help="Full path to SampleSheet, cross check list of Samples", default=None)
    parser.add_option("-p", "--purge-results", dest="purgeAnalysis", help="[NOT FUNCTIONAL]Purge the variants from the CVR database, this should be done as adminstrator of the CVR. Please be very careful using this option", default=None)

    (opts, args) = parser.parse_args()
    #
    ## get only version
    if opts.version: sys.exit(0)

    mAlysName = opts.alysName
    mOverwrite = opts.overwrite
    mAlysTaskId = opts.alysId
    mAlysResultsDir = opts.alysResultsDir
    mAnalystId = opts.personId
    mCnvOnly = opts.cnvOnly
    mSampleSheet = opts.ssFSLoc
    mPurge = opts.purgeAnalysis

    if mAlysResultsDir == None :
        print ("IMPACT analysis results directory is required for loading the variants into the database.\n\n")
        print "For usage call the CVR loader with -h/--help argument.\n"
        sys.exit(1)

    if mAlysTaskId == None or mAlysName == None:
        print ("DMP Analysis Task Name (usually like: IMPACTv3-CLIN-2014*) or a DMP Analysis Task Id (Numeric) \n"+
               "is needed for IMPACT analysis pipeline results is required to load the variants into the database.\n\n")
        print "For usage call the CVR loader with -h/--help argument.\n"
        sys.exit(1)

    #if mTitleFile == None:
    #    print ("A title file including meta-data information for the analysis task needs to be specified to load the results into\n"+
    #           "the CVR database. Please re-try by giving proper title file corresponding to this results.")

    mLogger = DMPLogger()
    mLogger = mLogger.getCVRLogger("dmp_cvr_load")
    mLogger.debug("Invoking CVR loader...")
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
        mLogger.debug("You have chosen to overwrite the CVR variants from previous analysis (if any). Be cautious of this action.")


    sys.exit(0) if main(mLogger, mOverwrite, mAlysName, mAlysTaskId, mAlysResultsDir, mCnvOnly) else sys.exit(1)


