'''
Created on Oct 28, 2014

@author: syeda1
'''

from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb
from optparse import OptionParser
import sys, os
from mskcc.dmp.cvr.utils.generic_utility import (getSysArchitecture, getUserName, getHostName )
from mskcc.dmp.cvr.dao.base import ORMBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DatabaseError, SQLAlchemyError, InvalidRequestError
from mskcc.dmp.cvr.dao.dmp_analysis_dao import Alys2Sample
from mskcc.dmp.cvr.dao.dmp_sample import DMPSample


class SVFieldIndex(object):
    TUMORID_NAME = 'TumorId'
    NORMALID_NAME = 'NormalId'
    CHROMOSOME1_NAME = 'Chr1'
    CHROM1_POS_NAME = 'Pos1'
    CHROMOSOME2_NAME = 'Chr2'
    CHROM2_POS_NAME = 'Pos2'
    SV_TYPE_NAME = 'SV_Type'
    GENE1_NAME = 'Gene1'
    GENE2_NAME = 'Gene2'
    SITE1DESC_NAME = 'Site1Description'
    SITE2DESC_NAME = 'Site2Description'
    FUSION_NAME = 'Fusion'
    CONFIDENCE_NAME = 'Confidence'
    COMMENTS_NAME = 'Comments'
    CONNECTION_NAME = 'Connection_Type'
    SV_LEN_NAME = 'SV_LENGTH'
    MAPQ_NAME = 'MAPQ'
    PE_READS_NAME = 'PairEndReadSupport'
    SPLIT_READS_NAME = 'SplitReadSupport'
    BRKPT_NAME = 'BrkptType'
    CONSENSUS_NAME = 'ConsensusSequence'
    TUMOR_VARIANT_NAME = 'TumorVariantCount'
    TUMOR_READ_NAME = 'TumorReadCount'
    TUMOR_GENO_QSCORE_NAME = 'TumorGenotypeQScore'
    NORMAL_VARIANT_CNT_NAME= 'NormalVariantCount'
    NORMAL_READ_CNT_NAME = 'NormalReadCount'
    NORMAL_GENO_QSCORE_NAME = 'NormalGenotypeQScore'
                                                        
    __fieldsMap = None
    
    def __init__(self, line):
        self.__fieldsMap = dict()
        self.__fieldsMap['TumorId'] = True
        self.__fieldsMap['NormalId'] = True
        self.__fieldsMap['Chr1'] = True
        self.__fieldsMap['Pos1'] = True
        self.__fieldsMap['Chr2'] = True
        self.__fieldsMap['Pos2'] = True
        self.__fieldsMap['SV_Type'] = True
        self.__fieldsMap['Gene1'] = True
        self.__fieldsMap['Gene2'] = True
        self.__fieldsMap['Site1Description'] = True
        self.__fieldsMap['Site2Description'] = True
        self.__fieldsMap['Fusion'] = True
        self.__fieldsMap['Confidence'] = True
        self.__fieldsMap['Comments'] = True
        self.__fieldsMap['Connection_Type'] = True
        self.__fieldsMap['SV_LENGTH'] = True
        self.__fieldsMap['MAPQ'] = True
        self.__fieldsMap['PairEndReadSupport'] = True
        self.__fieldsMap['SplitReadSupport'] = True
        self.__fieldsMap['BrkptType'] = True
        self.__fieldsMap['ConsensusSequence'] = True
        self.__fieldsMap['TumorVariantCount'] = True
        self.__fieldsMap['TumorReadCount'] = True
        self.__fieldsMap['TumorGenotypeQScore'] = True
        self.__fieldsMap['NormalVariantCount'] = True
        self.__fieldsMap['NormalReadCount'] = True
        self.__fieldsMap['NormalGenotypeQScore'] = True

        lAry = line.split("\t")
        counter = 0
        for field in lAry:
            self.__fieldsMap[field] = counter
            counter = counter+1
        
    def getTumorIdIndex(self):
        return self.__fieldsMap[self.TUMORID_NAME]
    
    def getNormalIdIndex(self):
        return self.__fieldsMap[self.NORMALID_NAME]
    
    def getChr1IdIndex(self):
        return self.__fieldsMap[self.CHROMOSOME1_NAME]
    
    def getChr2IdIndex(self):
        return self.__fieldsMap[self.CHROMOSOME2_NAME]
    
    def getPos1IdIndex(self):
        return self.__fieldsMap[self.CHROM1_POS_NAME]
    
    def getPos2IdIndex(self):
        return self.__fieldsMap[self.CHROM2_POS_NAME]
    
    def getSVTypeIdIndex(self):
        return self.__fieldsMap[self.SV_TYPE_NAME]
    
    def getGene1IdIndex(self):
        return self.__fieldsMap[self.GENE1_NAME]
    
    def getGene2IdIndex(self):
        return self.__fieldsMap[self.GENE2_NAME]
    
    def getSite1IdIndex(self):
        return self.__fieldsMap[self.SITE1DESC_NAME]
    
    def getSite2IdIndex(self):
        return self.__fieldsMap[self.SITE2DESC_NAME]
    
    def getFusionIdIndex(self):
        return self.__fieldsMap[self.FUSION_NAME]
    
    def getConfIdIndex(self):
        return self.__fieldsMap[self.CONFIDENCE_NAME]
    
    def getCommentsIdIndex(self):
        return self.__fieldsMap[self.COMMENTS_NAME]
    
    def getConnTypeIdIndex(self):
        return self.__fieldsMap[self.CONNECTION_NAME]
    
    def getSVLenIdIndex(self):
        return self.__fieldsMap[self.SV_LEN_NAME]
    
    def getMAPQIdIndex(self):
        return self.__fieldsMap[self.MAPQ_NAME]
    
    def getPEReadIdIndex(self):
        return self.__fieldsMap[self.PE_READS_NAME]
    
    def getSRIdIndex(self):
        return self.__fieldsMap[self.SPLIT_READS_NAME]
    
    def getBRKPTIdIndex(self):
        return self.__fieldsMap[self.BRKPT_NAME]
    
    def getConsensusIdIndex(self):
        return self.__fieldsMap[self.CONSENSUS_NAME]
    
    def getTVCountIdIndex(self):
        return self.__fieldsMap[self.TUMOR_VARIANT_NAME]
    
    def getTRCountIdIndex(self):
        return self.__fieldsMap[self.TUMOR_READ_NAME]
    
    def getTGenotypeQScoreIdIndex(self):
        return self.__fieldsMap[self.TUMOR_GENO_QSCORE_NAME]
    
    def getNVCountIdIndex(self):
        return self.__fieldsMap[self.NORMAL_VARIANT_CNT_NAME]
    
    def getNRCountIdIndex(self):
        return self.__fieldsMap[self.NORMAL_READ_CNT_NAME]
    
    def getNGenotypeQScoreIdIndex(self):
        return self.__fieldsMap[self.NORMAL_GENO_QSCORE_NAME]



class SVariant(object):
    __tumorId = None
    __normalId = None
    __loadStatus = None
    __sv_variant_id = -1
    __sv_class_cv_id = -1
    __sv_tool_cv_id = -1
    __site1_chrom = None
    __site2_chrom = None
    __site1_pos = None
    __site2_pos = None
    __site1_desc = None
    __site2_desc = None
    __event_info = None
    __conn_type = None
    __sv_length = None
    __sv_desc = None
    __median_mapq = -1
    __bkpt_type = 0
    __tumor_ad_pe = -1
    __tumor_ad_sr = -1
    __tumor_ad = -1
    __tumor_dp = -1
    __normal_dp = -1
    __normal_ad_pe = -1
    __normal_ad_sr = -1
    __normal_ad = -1
    __gene1 = None
    __gene2 = None
    __transcript1 = None
    __transcript2 = None
    __comments = None
    __confidence_cv_id = -1
    __variant_status_cv_id = -1
    __is_manually_loaded = -1
    __dt_last_modified = None
    __logger = None
    
    def __init__(self, logger, a2sId, classId, toolId, filterId, chrom1, chrom2, pos1, pos2, desc1, desc2, event, conn, svLen, svDesc, 
                 mapq, bkpt, tad_pe, tad_sr, tad, tdp, nad_pe, nad_sr, nad, ndp, gene1, gene2, trans1, trans2, cmts, confId, 
                 statusId, isMan, tumorId, normalId):
        self.__logger = logger
        self.__alys2sample_id = a2sId
        self.__sv_class_cv_id = classId
        self.__sv_tool_cv_id = toolId
        self.__sv_filter_cv_id = filterId
        self.__site1_chrom = chrom1
        self.__site2_chrom = chrom2
        self.__site1_desc = desc1
        self.__site2_desc = desc2
        self.__site2_pos = pos2
        self.__site1_pos = pos1
        self.__event_info = event
        self.__conn_type = conn
        self.__sv_length = svLen
        self.__sv_desc = svDesc
        self.__median_mapq = mapq
        self.__bkpt_type = bkpt
        self.__tumor_ad_pe = 0 if tad_pe == '' else tad_pe
        self.__tumor_ad_sr = 0 if tad_sr == '' else tad_sr
        self.__tumor_ad =  0 if tad == '' else tad
        self.__tumor_dp =  0 if tdp == '' else tdp
        self.__normal_ad_pe =  0 if nad_pe == '' else nad_pe
        self.__normal_ad_sr =  0 if nad_sr == '' else nad_sr
        self.__normal_ad =  0 if nad == '' else nad
        self.__normal_dp =  0 if ndp == '' else ndp
        self.__gene1 = gene1
        self.__gene2 = gene2
        self.__transcript1 = trans1
        self.__transcript2 = trans2
        self.__comments = cmts
        self.__confidence_cv_id = confId
        self.__variant_status_cv_id = statusId
        self.__is_manually_loaded = isMan
        self.__tumorId = tumorId
        self.__normalId = normalId
        #self.__dt_last_modified = dtMod

    
    def sync(self):
        sql = None
        dbObj = None
        retVal = True
        logger = self.__logger

        try:
            sql = """
             INSERT INTO structural_variants (alys2sample_id, sv_class_cv_id, sv_tool_cv_id, sv_filter_cv_id, 
             site1_chrom, site2_chrom, site1_pos, site2_pos,site1_desc, site2_desc,event_info,conn_type,
             sv_length,sv_desc,bkpt_type,tumor_ad_pe,tumor_ad_sr,tumor_ad,tumor_dp,normal_dp, normal_ad, site1_gene,
             site2_gene,comments, confidence_cv_id, is_manually_loaded, variant_status_cv_id, site1_transcript, site2_transcript) VALUES 
             (%s, %s, %s, %s, '%s','%s',%s,%s,'%s','%s','%s','%s',%s, '%s','%s',%s,%s,%s,%s,%s,%s,'%s','%s','%s',%s,%s, 1, '','')
            """%(
             self.__alys2sample_id,
             self.__sv_class_cv_id,
             self.__sv_tool_cv_id,
             self.__sv_filter_cv_id,
             self.__site1_chrom,
             self.__site2_chrom,
             self.__site1_pos,
             self.__site2_pos,
             self.__site1_desc,
             self.__site2_desc,
             self.__event_info,
             self.__conn_type,
             self.__sv_length,
             self.__sv_desc,
             self.__bkpt_type,
             self.__tumor_ad_pe,
             self.__tumor_ad_sr,
             self.__tumor_ad,
             self.__tumor_dp,
             self.__normal_dp,
             self.__normal_ad,
             self.__gene1,
             self.__gene2,
             self.__comments,
             self.__confidence_cv_id,
             self.__is_manually_loaded
            )
        except Exception as e:
            logger.error(e)
            return False
        
        logger.debug(sql.replace("\n"," ")+";")
        #return True
        #print sql.replace("\n"," ")+";"
        try:
            prop = Properties()
            dbObj = DMPdb(prop.getCVRDBUser(), prop.getCVRDBPass(), prop.getCVRDBServer(), prop.getCVRDBName())
            dbObj.connect()
            dbObj.executeInsert(sql)
            #self.__sv_variant_id = dbObj.getLastInsertId()
        except Exception as e:
            
            logger.error(e)
            retVal = False
        finally:
            if dbObj != None: dbObj.disconnect()
            
        return retVal

    
    def getTumorId(self):
        return self.__tumorId
    
    def getNormalId(self):
        return self.__normalId
    
    def getGene1(self):
        return self.__gene1
    
    def getGene2(self):
        return self.__gene2
    
    def getSite1Pos(self):
        return self.__site1_pos
    
    def getSite2Pos(self):
        return self.__site2_pos
    
    def getSite1Chrom(self):
        return self.__site1_chrom
    
    def getSite2Chrom(self):
        return self.__site2_chrom
    
    def retrieve(self):
        pass
    
    
class SVLoader(object):
    DONT_LOAD_STATUS = 2
    GOOD_TO_LOAD_STATUS = 1
    
    __svFile = None
    __fileHandle = None
    __svIndexObj = None
    __logger = None
    __svList = None
    __sToA2sMap = None
    __loadStatus = None
    __propObj = None
    __alysName = None
    __a2sNormalMap = None
    
    def __init__(self, svFP, logger):
        self.__svFile = svFP
        self.__svList = list()
        self.__sToA2sMap = dict()
        self.__loadStatus = self.DONT_LOAD_STATUS
        self.__propObj = Properties()
        self.__a2sNormalMap = dict()
        self.__logger = logger
        try:
            self.__alysName = os.path.basename(svFP).split("_")[0]
            if self.__alysName == None: return None # No object should be created
            self.__fileHandle = open(self.__svFile, 'rb')
            self.__svIndexObj = SVFieldIndex(self.__fileHandle.readline())
        except Exception as e:
            logger.error(e)
            raise "Exception while opening the file %s"%(self.__svFile)
            return None
        

    def getClass(self, className):
        if className == "INV": return 1
        if className == "TRA": return 2
        if className == "DEL": return 6
        if className == "DUP": return 4
        return 10


    def getConfidenceId(self, confStr):
        if confStr == "PRECISE": return 3
        if confStr == "IMPRECISE": return 2    
        return 2
    
    
    def parse(self):
        logger = self.__logger
        line = self.__fileHandle.readline()
        svIndexObj = self.__svIndexObj
        while line != None:
            lAry = line.split("\t")
            #a2sId, classId, toolId, filterId, chrom1, chrom2, pos1, pos2, desc1, desc2, event, conn, svLen, svDesc, mapq, bkpt, tad_pe, 
            #tad_sr, tad, tdp, nad_pe, nad_sr, nad, ndp, gene1, gene2, trans1, trans2, cmts, confId, statusId, isMan, dtMod
            try:
                logger.debug("SampleId: %s"%(lAry[svIndexObj.getTumorIdIndex()]).split("_")[0])
                a2sId = self.__getA2SId(lAry[svIndexObj.getTumorIdIndex()].split("_")[0])
                if lAry[svIndexObj.getTumorIdIndex()].split("_")[0] == '': continue
                
                if a2sId == -1:
                    logger.error("At least one of the samples did not have a A2S Id, so it is not safe to Sync to the database.")
                    return False
                svObj = SVariant(logger,
                                 a2sId,
                                 self.getClass(lAry[svIndexObj.getSVTypeIdIndex()]),
                                 1,
                                 1,
                                 lAry[svIndexObj.getChr1IdIndex()],
                                 lAry[svIndexObj.getChr2IdIndex()],
                                 lAry[svIndexObj.getPos1IdIndex()],
                                 lAry[svIndexObj.getPos2IdIndex()],
                                 lAry[svIndexObj.getSite1IdIndex()].replace("'",''),
                                 lAry[svIndexObj.getSite2IdIndex()].replace("'",''),
                                 lAry[svIndexObj.getFusionIdIndex()].replace("'",''),
                                 lAry[svIndexObj.getConnTypeIdIndex()].replace("'",''),
                                 lAry[svIndexObj.getSVLenIdIndex()],
                                 "",
                                 lAry[svIndexObj.getMAPQIdIndex()],
                                 lAry[svIndexObj.getBRKPTIdIndex()].replace("'",''),
                                 lAry[svIndexObj.getPEReadIdIndex()],
                                 lAry[svIndexObj.getSRIdIndex()],
                                 lAry[svIndexObj.getTVCountIdIndex()],
                                 lAry[svIndexObj.getTRCountIdIndex()],
                                 None,
                                 None,
                                 lAry[svIndexObj.getNVCountIdIndex()],
                                 lAry[svIndexObj.getNRCountIdIndex()],
                                 lAry[svIndexObj.getGene1IdIndex()],
                                 lAry[svIndexObj.getGene2IdIndex()],
                                 None,
                                 None,
                                 "",
                                 2,#self.getConfidenceId(lAry[svIndexObj.getConfIdIndex()]),
                                 1,
                                 0,
                                 lAry[svIndexObj.getTumorIdIndex()],
                                 lAry[svIndexObj.getNormalIdIndex()]
                                 )
                self.__svList.append(svObj)
                self.__a2sNormalMap[a2sId] = lAry[svIndexObj.getNormalIdIndex()]
                line = self.__fileHandle.readline()
                if line == None or line == '': break
            except EOFError as e:
                break
            except Exception as e:
                logger.error(e)
                return False
        #end of while
        self.__loadStatus = self.GOOD_TO_LOAD_STATUS
        return True
        

    def __getA2SId(self, sampleId):
        dbObj = None
        logger = self.__logger
        retVal = True
        sql = "SELECT a2s.alys2sample_id FROM alys2sample a2s, dmp_alys_task at, dmp_sample ds WHERE a2s.dmp_sample_id=ds.dmp_sample_id AND a2s.dmp_alys_task_id=at.dmp_alys_task_id AND at.dmp_alys_task_name='%s' AND ds.lims_sample_id='%s'"%(self.__alysName, sampleId)
        
        logger.debug(sql)
        try:
            prop = Properties()
            dbObj = DMPdb(prop.getCVRDBUser(), prop.getCVRDBPass(), prop.getCVRDBServer(), prop.getCVRDBName())
            dbObj.connect()
            dbObj.execute(sql)
            res = dbObj.fetchone()
            retVal = res['alys2sample_id']
        except Exception as e:
            logger.error(e)
            retVal = False
        finally:
            if dbObj != None: dbObj.disconnect()
            
        return retVal
        
        
        """
        logger = self.__logger
        if self.__sToA2sMap.has_key(sampleId): return self.__sToA2sMap[sampleId]
        
        session = None
        retVal = -1
        try:
            Session = sessionmaker(bind = ORMBase.globalEngine)
            session = Session()
            logger.debug("Retrieving the A2SId for Sample: %s"%(sampleId))
            res = session.query(DMPSample, Alys2Sample).filter(DMPSample.dmp_sample_id==Alys2Sample.dmp_sample_id).filter(DMPSample.lims_sample_id==sampleId)
            logger.debug(res)
            if res != None:
                retVal = res[1].alys2sample_id
        except DatabaseError as e:
            logger.error(e)
            logger.error("DatabaseError. while retrieving the analysis %s"%(sampleId))
            retVal  = -1
        except SQLAlchemyError as e:
            logger.error(e)
            logger.error("SQLAlchemyError.  while retrieving the analysis %s"%(sampleId))
            retVal  = -1
        except InvalidRequestError as e:
            logger.error(e)
            logger.error("InvalidRequestError. while retrieving the analysis %s"%(sampleId))
            retVal  = -1
        except Exception as e:
            logger.error(e)
        finally:
            if(session != None):
                session.close()
        if retVal != -1: self.__sToA2sMap[sampleId] = retVal
        return retVal
    """
    
    
    def load(self):
        logger = self.__logger
        if self.__loadStatus == self.DONT_LOAD_STATUS:
            logger.error("Cant load as the something might have failed earlier...")
            return False
        for svObj in self.__svList:
            svObj.sync()
            #self.createIGVFile(svObj.getTumorId(), svObj.getNormalId(), svObj.getGene1(), svObj.getSite1Pos(), svObj.getSite1Chrom())
            #self.createIGVFile(svObj.getTumorId(), svObj.getNormalId(), svObj.getGene2(), svObj.getSite2Pos(), svObj.getSite2Chrom())
            
        #for a2sId, normalId in self.__a2sNormalMap.items():
        #    self.__syncA2SNormal(a2sId, normalId.split("_")[0])
            
        return True
    
    
    def __syncA2SNormal(self, a2sId, normalId):
        dbObj = None
        logger = self.__logger
        retVal = True
        sql = "INSERT INTO a2s_params (alys2sample_id, param_key_cv_id, param_value) VALUES (%s, %s, '%s')"%(a2sId, 6, normalId)
        
        logger.debug( sql)
        #return True
    
        try:
            prop = Properties()
            dbObj = DMPdb(prop.getCVRDBUser(), prop.getCVRDBPass(), prop.getCVRDBServer(), prop.getCVRDBName())
            dbObj.connect()
            dbObj.executeInsert(sql)
            self.__sv_variant_id = dbObj.getInsertId()
        except Exception as e:
            logger.error(e)
            retVal = False
        finally:
            if dbObj != None: dbObj.disconnect()
            
        return retVal
    
    def createIGVFile(self, sampleId, normalId, geneId, startPosition, chrom):
        logger = self.__logger
        propObj = self.__propObj
        dataFSLoc = propObj.getCVRDataFSLoc()
        staticURL = propObj.getStaticURLRoot()
        tumorBAM = "%s/%s/bam/%s.bam"%(dataFSLoc,self.__alysName,sampleId.split("_")[0])
        normalBAM = "%s/%s/bam/%s.bam"%(dataFSLoc,self.__alysName,normalId.split("_")[0])
        tumorBAM = tumorBAM.replace(dataFSLoc, staticURL)
        normalBAM = normalBAM.replace(dataFSLoc, staticURL)

        session_file_name = "%s_%s_%s.xml"%(sampleId.split("_")[0], geneId, startPosition)
        session_file_fs =  "%s/%s/igv/"%(dataFSLoc,self.__alysName) +session_file_name
        
        logger.debug( "SESSION FILE: %s  -  %s  - %s  - %s  - %s"%(session_file_fs, chrom, startPosition, tumorBAM, normalBAM))
        #return True
    
        with open(session_file_fs, 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n')
            f.write('<Session genome="hg19" locus="' + "%s:%s-%s"%(chrom, startPosition, startPosition) + '" version="5">\n')
            f.write('<Resources>\n')
            f.write('<Resource name="Tumor - ' + sampleId + '" path="' + tumorBAM + '"/>\n')
            f.write('<Resource name="Normal - '  + normalId + '" path="' + normalBAM + '"/>\n')
            f.write('</Resources>\n')
            f.write('</Session>\n')
        return True
    
    

if __name__ == '__main__':
    parser = OptionParser()
    parser.set_description("This CLI is used to load the structural variant results from the IMPACT pipeline into the CVR database. Please be very careful in setting up the PYTHONPATH and database connectivity.")
    parser.add_option("-i", "--sv-file", dest="svFile", help="[REQUIRED] Full qualified file path to the SV results file", default=None)
    
    (opts, args) = parser.parse_args()
    
    msvFile = opts.svFile
    mLogger = DMPLogger()
    mLogger = mLogger.getCVRLogger("dmp_sv_load")
    mLogger.debug("Invoking CVR loader...")
    mLogger.debug("User: %s \nHostname: %s \nSystemArchitecture: %s"%(getUserName(), getHostName(), getSysArchitecture()))
    
    svLoadObj = SVLoader(msvFile, mLogger)
    svLoadObj.parse()
    svLoadObj.load()
