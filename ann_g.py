from mskcc.dmp.cvr.utils.dmp_singleton import Singleton
from mskcc.dmp.cvr.dao.controlled_vocab import (ConfidenceCV, SNPIndelToolCV, SNPIndelFilterCV, ParamKeyCV,
                                              VariantStatusCV, CNVClassCV, CNVFilterCV, TumorTypeCV, VariantClassCV)
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.dao.dmp_variant_dao import DMPVariants
from sqlalchemy.exc import DatabaseError, SQLAlchemyError, InvalidRequestError, IntegrityError
from sqlalchemy.orm import sessionmaker
from mskcc.dmp.cvr.dao.base import ORMBase
from mskcc.dmp.cvr.utils.decorators import timer


class SNPIndelFileColumnIndex(object):

    __index2NameMap = None
    __name2IndexMap = None
    def __init__(self, header=None):
        # Sample  NormalUsed      Chrom   Start   Ref     Alt     VariantClass    Gene    Exon    Call_Confidence Comments
        # TranscriptID    cDNAchange      AAchange dbSNP_ID        Cosmic_ID 1000G_MAF       FailureReason   N_TotalDepth
        # N_RefCount      N_AltCount      N_AltFreq       T_TotalDepth    T_RefCount      T_AltCount T_AltFreq
        # T_Ref+  T_Ref-  T_Alt+  T_Alt-  All_N_Aggregate_AlleleDepth     All_N_Median_AlleleFreq T_freq/All_N_Freq
        # Occurence_in_Normals

        self.__name2IndexMap = dict()
        self.__index2NameMap = dict()
        if header != None:
            hAry = header.split("\t")
            counter = 0
            for hIndex in hAry:
                if hIndex == 'T_freq/All_N_Freq':
                    self.__index2NameMap['T_freq_All_N_Freq'] = counter
                else:
                    self.__index2NameMap[hIndex] = counter
                counter = counter + 1
        else:
            self.__index2NameMap['Sample'] = 0
            self.__index2NameMap['NormalUsed'] = 1
            self.__index2NameMap['Chrom'] = 2
            self.__index2NameMap['Start'] = 3
            self.__index2NameMap['Ref'] = 4
            self.__index2NameMap['Alt'] = 5
            self.__index2NameMap['VariantClass'] = 6
            self.__index2NameMap['Gene'] = 7
            self.__index2NameMap['Exon'] = 8
            self.__index2NameMap['Call_Confidence'] = 9
            self.__index2NameMap['Comments'] = 10
            self.__index2NameMap['TranscriptID'] = 11
            self.__index2NameMap['cDNAchange'] = 12
            self.__index2NameMap['AAchange'] = 13
            self.__index2NameMap['dbSNP_ID'] = 14
            self.__index2NameMap['Cosmic_ID'] = 15
            self.__index2NameMap['1000G_MAF'] = 16
            self.__index2NameMap['FailureReason'] = 17
            self.__index2NameMap['CallMethod'] = 18
            self.__index2NameMap['COSMIC_site'] = 19
            self.__index2NameMap['N_TotalDepth'] = 20
            self.__index2NameMap['N_RefCount'] = 21
            self.__index2NameMap['N_AltCount'] = 22
            self.__index2NameMap['N_AltFreq'] = 23
            self.__index2NameMap['T_TotalDepth'] = 24
            self.__index2NameMap['T_RefCount'] = 25
            self.__index2NameMap['T_AltCount'] = 26
            self.__index2NameMap['T_AltFreq'] = 27
            self.__index2NameMap['T_Ref+'] = 28
            self.__index2NameMap['T_Ref-'] = 29
            self.__index2NameMap['T_Alt+'] = 30
            self.__index2NameMap['T_Alt-'] = 31
            self.__index2NameMap['All_N_Aggregate_AlleleDepth'] = 32
            self.__index2NameMap['All_N_Median_AlleleFreq'] = 33
            self.__index2NameMap['T_freq_All_N_Freq'] = 34 # T_freq/All_N_Freq
            self.__index2NameMap['Occurence_in_Normals'] = 35



    def getSamplePos(self): return self.__index2NameMap['Sample']
    def getNormalPos(self): return self.__index2NameMap['NormalUsed']
    def getChromPos(self): return self.__index2NameMap['Chrom']
    def getStartPos(self): return self.__index2NameMap['Start']
    def getRefAllelePos(self): return self.__index2NameMap['Ref']
    def getAltAllelePos(self): return self.__index2NameMap['Alt']
    def getVariantClassPos(self): return self.__index2NameMap['VariantClass']
    def getGenePos(self): return self.__index2NameMap['Gene']
    def getExonPos(self): return self.__index2NameMap['Exon']
    def getCallConfPos(self): return self.__index2NameMap['Call_Confidence']
    def getCommentPos(self): return self.__index2NameMap['Comments']
    def getTranscriptPos(self): return self.__index2NameMap['TranscriptID']
    def getcDNAPos(self): return self.__index2NameMap['cDNAchange']
    def getAAPos(self): return self.__index2NameMap['AAchange']
    def getdbSNPPos(self): return self.__index2NameMap['dbSNP_ID']
    def getCosmicPos(self): return self.__index2NameMap['Cosmic_ID']
    def getMinorAlleleFreqPos(self): return self.__index2NameMap['1000G_MAF']
    def getFilterPos(self): return self.__index2NameMap['FailureReason']
    def getNormalTotalRDPos(self): return self.__index2NameMap['N_TotalDepth']
    def getNormalRefRDPos(self): return self.__index2NameMap['N_RefCount']
    def getNormalAltRDPos(self): return self.__index2NameMap['N_AltCount']
    def getNormalAltFreqPos(self): return self.__index2NameMap['N_AltFreq']
    def getTumorTotalRDPos(self): return self.__index2NameMap['T_TotalDepth']
    def getTumorRefRDPos(self): return self.__index2NameMap['T_RefCount']
    def getTumorAltRDPos(self): return self.__index2NameMap['T_AltCount']
    def getTumorAltFreqPos(self): return self.__index2NameMap['T_AltFreq']
    def getTumorRefRDCisPos(self): return self.__index2NameMap['T_Ref+']
    def getTumorRefRDTransPos(self): return self.__index2NameMap['T_Ref-']
    def getTumorAltRDCisPos(self): return self.__index2NameMap['T_Alt+']
    def getTumorAltRDTransPos(self): return self.__index2NameMap['T_Alt-']
    def getNAggADPos(self): return self.__index2NameMap['All_N_Aggregate_AlleleDepth']
    def getNMedianAllelFreqPos(self): return self.__index2NameMap['All_N_Median_AlleleFreq']
    def getT2AllNFreqRatioPos(self): return self.__index2NameMap['T_freq_All_N_Freq']
    def getOccurenceNormalsPos(self): return self.__index2NameMap['Occurence_in_Normals']
    def getCallMethodPos(self): return self.__index2NameMap['CallMethod']
    def getCOSMICSitePos(self): return self.__index2NameMap['COSMIC_site']
    #def getBlackList_G(self):pass

class G_SNPIndelFileColumnIndex(SNPIndelFileColumnIndex):
            #__index2NameMap_G= None    #_SNPIndelFileColumnIndex__index2NameMap
            #__name2IndexMap_G= None #SNPIndelFileColumnIndex.__bases__.__name2IndexMap
            __SNP_HEAD = None


            def __init__(self, head=None):
                self.__SNP_HEAD =['Sample', 'CallMethod', 'Chrom', 'Start', 'Ref', 'Alt',
                        'VariantClass', 'Gene', 'Exon', 'Call_Confidence', 'Comments',
                        'BlackList', 'TranscriptID', 'cDNAchange', 'AAchange', 'dbSNP_ID',
                        'Cosmic_ID', '1000G_MAF', 'ESP', 'SIFT_score', 'SIFT_pred',
                        'PolyPhen2_score', 'PolyPhen2_pred', 'MutationTaster_score',
                        'MutationTaster_pred', 'MutationAssessor_score', 'MutationAssessor_Pred',
                        'MetaRadialSVM_score', 'MetaRadialSVM_Pred', 'MetaLR__score', 'MetaLR_pred',
                        'GERP_score', 'PhyloP_score', 'CLINVAR', 'PATH_SCORE', 'PATH_SCORE_reasons',
                        'FailureReason', 'T_TotalDepth', 'T_RefCount', 'T_AltCount', 'T_AltFreq',
                        'T_Ref+', 'T_Ref-', 'T_Alt+', 'T_Alt-']
                super(G_SNPIndelFileColumnIndex,self).__init__(head)
                self.__index2NameMap_G = self._SNPIndelFileColumnIndex__index2NameMap#dict()
                self.__name2IndexMap_G = self._SNPIndelFileColumnIndex__name2IndexMap#dict()
                #self.__name2IndexMap= dict()
                #if head == None: #override
                for indx, e in enumerate(self.__SNP_HEAD):
                    self.__index2NameMap_G[e] = indx
                    #self._SNPIndelFileColumnIndex__index2NameMap[e] = indx
                    self.__name2IndexMap_G[indx] = e

            #new added columns in Germline_Annotated_File
            def getBlackList_GPos(self):
                return self.__index2NameMap_G['BlackList']
                #return self._SNPIndelFileColumnIndex__index2NameMap['BlackList']
            def getESP_GPos(self):
                return self.__index2NameMap_G['ESP']

            def getSIFTscore_GPos(self):
                return self.__index2NameMap_G['SIFT_score']

            def getSIFTpred_GPos(self):
                return self.__index2NameMap_G['SIFT_pred']

            def getPolyPhen2_score_GPos(self):
                return self.__index2NameMap_G['PolyPhen2_score']

            def getPolyPhen2_pred_GPos(self):
                return self.__index2NameMap_G['PolyPhen2_pred']

            def getMutationTaster_score_GPos(self):
                return self.__index2NameMap_G['MutationTaster_score']

            def getMutationAssessor_score_GPos(self):
                return self.__index2NameMap_G['MutationAssessor_score']

            def getMutationAssessor_Pred_GPos(self):
                return self.__index2NameMap_G['MutationAssessor_Pred']

            def getMutationTaster_pred_GPos(self):
                return self.__index2NameMap_G['MutationTaster_pred']

            def getMetaRadialSVM_score_GPos(self):
                return self.__index2NameMap_G['MetaRadialSVM_score']

            def getMetaRadialSVM_Pred_GPos(self):
                return self.__index2NameMap_G['MetaRadialSVM_Pred']

            def getMetaLR__score_GPos(self):
                return self.__index2NameMap_G['MetaLR__score']

            def getMetaLR_pred_GPos(self):
                return self.__index2NameMap_G['MetaLR_pred']

            def getGERP_score_GPos(self):
                return self.__index2NameMap_G['GERP_score']

            def getPhyloP_score_GPos(self):
                return self.__index2NameMap_G['PhyloP_score']

            def getCLINVAR_GPos(self):
                return self.__index2NameMap_G['CLINVAR']

            def getPATH_SCORE_GPos(self):
                return self.__index2NameMap_G['PATH_SCORE']

            def getPATH_SCORE_reasons_GPos(self):
                return self.__index2NameMap_G['PATH_SCORE_reasons']

            #def getFilterPos(self):
                #return self.__index2NameMap_G['FailureReason']
                #return super(G_SNPIndelFileColumnIndex,self).getFilterPos()


            #def getdbSNPPos(self): return super(G_SNPIndelFileColumnIndex,self).getdbSNPPos()
            #def getCosmicPos(self): return super(G_SNPIndelFileColumnIndex,self).getCosmicPos()
            #def getMinorAlleleFreqPos(self): return super(G_SNPIndelFileColumnIndex,self).getMinorAlleleFreqPos()
            '''
            def getFilterPos(self): return self.__index2NameMap['FailureReason']
            def getTumorTotalRDPos(self):
            def getTumorRefRDPos(self): return self.__index2NameMap['T_RefCount']
            def getTumorAltRDPos(self): return self.__index2NameMap['T_AltCount']
            def getTumorAltFreqPos(self): return self.__index2NameMap['T_AltFreq']
            def getTumorRefRDCisPos(self): return self.__index2NameMap['T_Ref+']
            def getTumorRefRDTransPos(self): return self.__index2NameMap['T_Ref-']
            def getTumorAltRDCisPos(self): return self.__index2NameMap['T_Alt+']
            def getTumorAltRDTransPos(self): return self.__index2NameMap['T_Alt-']
            def getCallMethodPos(self): return self.__index2NameMap['CallMethod']
            def getCOSMICSitePos(self): return self.__index2NameMap['COSMIC_site']
            '''




if __name__ == '__main__':
    g = G_SNPIndelFileColumnIndex()
    print g.__dict__
                #print SNPIndelFileColumnIndex.__class__
                #print g._SNPIndelFileColumnIndex__index2NameMap #superprivate and gets namemangled to prevent accidental access
    print g.getSamplePos(),g.getCallMethodPos(),g.getChromPos(),g.getStartPos(),g.getRefAllelePos(),g.getAltAllelePos(),g.getVariantClassPos(),g.getGenePos() ,g.getExonPos()
    print g.getCallConfPos(),g.getCommentPos(),g.getBlackList_GPos(),g.getTranscriptPos(),g.getcDNAPos()
    print g.getAAPos(),g.getdbSNPPos(),g.getCosmicPos(),g.getMinorAlleleFreqPos(),g.getESP_GPos()
    print g.getSIFTscore_GPos(),g.getSIFTpred_GPos(),
    print g.getPolyPhen2_score_GPos(),g.getPolyPhen2_pred_GPos(),
    print g.getMutationTaster_score_GPos(),g.getMutationTaster_pred_GPos(),g.getMutationAssessor_score_GPos(),g.getMutationAssessor_Pred_GPos()
    print g.getMetaRadialSVM_score_GPos(),g.getMetaRadialSVM_Pred_GPos()
                #print g.getMetaRadialSVM_Pred_G()
    print g.getMetaLR__score_GPos(),g.getMetaLR_pred_GPos(),g.getGERP_score_GPos()
    print g.getPhyloP_score_GPos(),g.getCLINVAR_GPos(),g.getPATH_SCORE_GPos(),g.getPATH_SCORE_reasons_GPos()
    print g.getFilterPos(),g.getTumorTotalRDPos(),g.getTumorRefRDPos(),g.getTumorAltRDPos()
    print g.getTumorAltFreqPos(),g.getTumorRefRDCisPos(),g.getTumorRefRDTransPos()
    print g.getTumorAltRDCisPos(),g.getTumorAltRDTransPos()
    print g.getNormalPos(),g.getT2AllNFreqRatioPos(),g.getOccurenceNormalsPos(),g.getNormalAltFreqPos(),g.getNormalTotalRDPos()
    print g.getNormalRefRDPos(),g.getNAggADPos(),g.getCOSMICSitePos(),g.getNormalAltRDPos(),g.getNMedianAllelFreqPos()


