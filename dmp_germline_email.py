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

Created on Sep 17, 2013

@author: wangy6,prasadm

'''
#!/dmp/resources/prod/tools/system/python/Python-2.7.3/bin/python

# -*- coding: utf-8 -*-
from optparse import OptionParser
from org.mskcc.dmp.dmutils.dmp_constants import GenericConstants, DMPCommands
from datetime import datetime
from org.mskcc.dmp.dmutils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.samplesheet import HiSeqSampleSheet
from org.mskcc.dmp.dmutils.generic_utility  import sendEmail, getYear
from org.mskcc.dmp.dmutils.dmp_logger import DMPLogger
import os, sys, getpass
from org.mskcc.dmp.dmutils.dmp_db import DMPdb
#from dmsqc.dmsqc_constants import (DMSAnalysisConstants)

def sendGLAlert(sample, project, annId, dmg,glAlysId,url,atId,baldspotUrl,a2sid,logger):
    logger.debug("Sending DMS-IMPACT-QC alert e-mail to the subscribed attendees")
    try:
        eMailList = 'zehira@mskcc.org,prasadm@mskcc.org,wangy6@mskcc.org,syeda1@mskcc.org,zhangl2@mskcc.org,liy7@mskcc.org,arguetas@mskcc.org,nauparis@mskcc.org'
        eMailList ='wangy6@mskcc.org'
        impactQCURL = "https://cbx-dms.mskcc.org:8443/cgi-bin/dmp_impact_qc_html.py?atId=%s&rdts=0"%(atId)
        htmlBody = "Hello all,<br><br>Germline-Analysis/Annotation Completed for sample %s<br><br>"%(sample)
        htmlBody = htmlBody+"<TABLE border=\"1\" cellpadding=\"15\">"
        htmlBody = htmlBody+"<TR><TD><b>SAMPLE Project</b></TD><TD>%s </TD></TR>"%(project)
        htmlBody = htmlBody+"<TR><TD><b>SAMPLE Name</b></TD><TD>%s </TD></TR>"%(sample)
        htmlBody = htmlBody+"<TR><TD><b>Germline-QC URL</b></TD><TD>%s </TD></TR>"%(impactQCURL)
        htmlBody = htmlBody+"<TR><TD><b>Inegnuity URL</b></TD><TD>%s </TD></TR>"%(url)
        htmlBody = htmlBody+"<TR><TD><b>Baldspot Analysis URL</b></TD><TD>%s </TD></TR>"%(baldspotUrl)
        htmlBody = htmlBody+"<TR><TD><b>CVR-Germline URL</b></TD><TD><a href='http://crater.mskcc.org:8082/sample/%s'>Sample View</TD></TR>"%(a2sid)
        htmlBody = htmlBody+"<TR><TD><b>DMG # </b></TD><TD>%s </TD></TR>"%(dmg)
        htmlBody = htmlBody+"<TR><TD><b>Germline Analysis Task ID</b></TD><TD>%s </TD></TR>"%(glAlysId)
        htmlBody = htmlBody+"<TR><TD><b>Germline Sample Annotation Task ID</b></TD><TD>%s </TD></TR>"%(annId)
        htmlBody = htmlBody+"</TABLE><br><br>"
        htmlBody = htmlBody+"Thanks!<br><br>"
        htmlBody = htmlBody+"If we are spamming you, please contact: prasadm@mskcc.org or wangy6@mskcc.org<br><br>"
        htmlBody = htmlBody+"""<font size="2" color="red">
               DISCLIAMER:Please note that this e-mail and any files transmitted from Memorial Sloan-Kettering Cancer Center may be privileged, confidential,<br>
               and protected from disclosure under applicable law. If the reader of  this message is not the intended recipient, or an employee or agent<br>
               responsible for delivering this message to the intended recipient, you are hereby notified that any reading, dissemination, distribution, <br>
               copying, or other use of this communication or any of its attachments is strictly prohibited.  If you have received this communication in <br>
               error, please notify the sender immediately by replying to this message and deleting this message, any attachments, and all copies and backups<br>
               from your computer.</font>"""
        logger.debug("About to send the e-mail")
        sendEmail("[DMS/CVR-GermlineAnalysis-Alert] Completed %s"%(sample), htmlBody, logger, eMailList)
        logger.debug("Done sending the Email...")
    except Exception as e:
        logger.error("Error %s occurred while sending the DMS alert e-mail for %s %s"%(e,dataDir, sample, project))
        return False
    return True

def sendGLoadFailedEmail(sample,logger,a2s_id=None,projectName=None):
    logger.debug("Sending DMS-IMPACT-Germline failed alert e-mail to the subscribed attendees")
    try:
        eMailList = 'prasadm@mskcc.org,wangy6@mskcc.org,syeda1@mskcc.org'
        eMailList ='wangy6@mskcc.org'
        subject = "[CVR-GERMLINE-FAILED-Alert] %s LOADING FAILED"%(sample)
        #htmlBody = "<br><br>Germline-Analysis/Annotation Failed for sample %s<br><br>"%(sample)
        sendEmail(subject, "Failed to load this Sample %s AT alys2sample_id  %s AT Project %s "%(sample,a2s_id,projectName), logger,eMailList)
    except Exception as e:
        logger.error("Error occurred while sending the DMS/CVRG alert e-mail for %s "%( sample))
        return False
    return True


def getAnnotationInfo(sample, antId, logger):
    sql = ""
    dmg = ""
    try:
        dbObj = DMPdb()
        dbObj.connect()
	result = None
        sql = "SELECT * FROM dmp_gl_annotation WHERE sample_name = '%s' and dmp_gl_annotation_id = '%s'"%(sample,antId)
        logger.debug(sql)
        dbObj.execute(sql)
        result = dbObj.fetchone()
        if result == None:
            return ("", "")
        dmg = result['dmg_number']
        glAnalysisId = result['dmp_gl_analysis_id']
        logger.debug("Success SQL: %s"%(sql))
    except:
        sendGLoadFailedEmail(sample,logger)
        logger.error("SQL %s Failed"%(sql))
        return ("","")
    finally:
        if(dbObj != None):
            dbObj.disconnect()
    return (dmg,glAnalysisId)

def getCVRDBHandle(logger):
    dbObj = None
    from mskcc.dmp.cvr.utils.dmp_properties import Properties
    propObj = Properties()
    logger.debug( "create CVR-G handler %s - %s - %s"%(propObj.getCVRDBUser(),propObj.getCVRDBServer(),propObj.getCVRDBName()))
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

def getGAnnotationInfo(dmp_assay_lbl,logger):#sample is the dmp_assay_lbl, in order to keep consistent with CVR-G loading process
    sql = ""
    dmg = ""
    a2sid = ""
    projectName = ''
    try:
        dbObj = getCVRDBHandle(logger)
        dbObj.connect()
        logger.debug("Successful connection to CVR-G")
        sqlGA2s ="select * from alys2sample a2s join dmp_alys_task  t on t.dmp_alys_task_id = a2s.dmp_alys_task_id where dmp_assay_lbl ='%s'"%(dmp_assay_lbl)
        logger.debug(sqlGA2s)
        dbObj.execute(sqlGA2s)
        cvrResult=dbObj.fetchone()
        logger.debug(cvrResult)
        a2sid = cvrResult['alys2sample_id']
        projectName = cvrResult['dmp_alys_task_name']
        logger.debug('a2sid %s,projectName%s'%(a2sid,projectName))
        #sql = "select * from dmp_gl_annotation gan JOIN dmp_gl_analysis gal ON gal.dmp_gl_analysis_id = gan.dmp_gl_analysis_id where sample_name = '%s' AND analysis_name = '%s'"%(sample,projectName)
        dbObj = DMPdb()
        dbObj.connect()
        #result = None
        sql="select * from dmp_gl_annotation gan JOIN dmp_gl_analysis gal ON gal.dmp_gl_analysis_id = gan.dmp_gl_analysis_id join dmp_sample s on s.lims_sample_id=gan.sample_name where dmp_assay_lbl = '%s' and analysis_name ='%s'"%(dmp_assay_lbl,projectName)
        logger.debug(sql)
        dbObj.execute(sql)
        result = dbObj.fetchone()
        #if result == None:
         #  return ("")
        dmg = result['dmg_number']
        glAnalysisId = result['dmp_gl_analysis_id']
        annId=result['dmp_gl_annotation_id']
        sample =result['sample_name']
        logger.debug('getGAnnotationInfo: %s, %s, %s,%s'%(sample,dmg,glAnalysisId,annId))
        logger.debug("Success SQL on dms: %s"%(sql))
        #query alys2sample_id
    except:
        logger.error("SQL %s,%s Failed"%(sqlGA2s,sql))
        sendGLoadFailedEmail(dmp_assay_lbl,logger,a2sid,projectName)
        return ("","")
    finally:
        if(dbObj != None):
            dbObj.disconnect()
    return (projectName,sample,a2sid,dmg,glAnalysisId,annId)


def getAnalysisTaskId(projectName, logger):
    sql = ""
    atId = ""
    try:
        dbObj = DMPdb()
        dbObj.connect()
        result = None
        sql = "SELECT * FROM dmp_rl_analysis WHERE analysis_name = '%s'"%(projectName)
        logger.debug(sql)
        dbObj.execute(sql)
        result = dbObj.fetchone()
        if result == None:
            return ("")
        atId = result['dmp_rl_analysis_id']
        logger.debug("Success SQL: %s"%(sql))
    except:
        logger.error("SQL %s Failed"%(sql))
        return ("")
    finally:
        if(dbObj != None):
            dbObj.disconnect()
    return (atId)

def getIngenuity(sample, projectName, logger):
    sql = ""
    url = ""
    try:
        dbObj = DMPdb()
        dbObj.connect()
        result = None
        sql = "SELECT inl.ingenuity_url FROM ingenuity_logs inl INNER JOIN  dmp_sample ds ON inl.sample_name = ds.dmp_assay_lbl WHERE lims_sample_id = '%s' and inl.sample_project = '%s'"%(sample,projectName)
        logger.debug(sql)
        dbObj.execute(sql)
        result = dbObj.fetchone()
        if result == None:
            return ("")
        url = result['ingenuity_url']
        logger.debug("Success SQL: %s"%(sql))
    except:
        sendGLoadFailedEmail(dmp_assay_lbl,logger)
        logger.error("SQL %s Failed"%(sql))
        return ("")
    finally:
        if(dbObj != None):
            dbObj.disconnect()
    return (url)

if __name__=="__main__":
    dmplogger = DMPLogger()
    mLogger = dmplogger.getIMPACTLogger("dmp_dms_germline_email")
    parser = OptionParser()
    parser.add_option("-s", "--dmp_assay_lbl", dest="samplelbl", help="sample name", default=None) # inorder to keep consistent with cvr g loading
    #parser.add_option("-p", "--project", dest="project", help="project name", default=None)
    #parser.add_option("-a", "--a2sid,", dest="a2sid", help="alys2sample_id in CVR-GERMLINE", default=None)
    (opts, args) = parser.parse_args()
    mSamplelbl = opts.samplelbl
    #mProject = opts.project
    #mA2SId = opts.a2sid
    #(dmg,glAlysId) = getAnnotationInfo(mSample, mAntId, mLogger)
    (mProject,mSample,mA2SId,dmg,glAlysId,mAntId) =getGAnnotationInfo(mSamplelbl,mLogger)
    url = getIngenuity(mSample, mProject, mLogger)
    atId = getAnalysisTaskId(mProject, mLogger)
    if dmg != "" and glAlysId != "" and url != "" and atId != "":
        baldspotUrl = "https://cbx-dms.mskcc.org:8443/cgi-bin/baldspot_html.py?atId=%s"%(atId)
        sendGLAlert(mSample,mProject,mAntId,dmg,glAlysId,url,atId,baldspotUrl,mA2SId,mLogger)
        #sendGLoadFailedEmail(mSamplelbl,mLogger,mA2SId,mProject)
    else:
        sendGLoadFailedEmail(mSamplelbl,mLogger,mA2SId,mProject)
        mLogger.error("Encountered an error sending email for %s and %s, %s,%s,%s,%s"%(mSample,mProject,dmg,glAlysId,url,atId))
        sys.exit(GenericConstants.FAILURE)
    sys.exit(GenericConstants.SUCCESS)

