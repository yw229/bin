from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from optparse import OptionParser
import sys, os
from mskcc.dmp.cvr.utils.generic_utility import ( getATSnpIndelsForSampleIGVFiles )
from mskcc.dmp.cvr.utils.dmp_properties import Properties



def createIGVFile(cvrFSLoc, sampleId, geneId, startPosition, chrom, projName):
    propObj = Properties()
    #dataFSLoc = propObj.getCVRDataFSLoc()
    #staticURL = propObj.getStaticURLRoot()
    #normalBAM = normalBAM.replace(dataFSLoc, staticURL)
    try:
        normalBAM = propObj.getStaticURLRoot() +  "/"+projName+"/bam/"+sampleId+".bam"
        tumorSampleId = sampleId.split('-')[0]+"-T"
        tumorBAM = propObj.getStaticURLRoot() +  "/"+projName+"/bam/"+tumorSampleId+".bam"

        session_file_name = "%s_%s_%s.xml"%(sampleId, geneId, startPosition)
        session_file_fs =  cvrFSLoc + "/igv/" + session_file_name
        #print session_file_fs
        with open(session_file_fs, 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n')
            f.write('<Session genome="hg19" locus="' + "%s:%s-%s"%(chrom, startPosition, startPosition) + '" version="5">\n')
            f.write('<Resources>\n')
            f.write('<Resource path="http://virgo1.mskcc.org:8081/intervals/abra_target_regions.bed"/>\n')
            f.write('<Resource name="Normal - '  + sampleId + '" path="' + normalBAM + '"/>\n')
            f.write('<Resource name="Tumor - '  + tumorSampleId + '" path="' + tumorBAM + '"/>\n')
            f.write('</Resources>\n')
            f.write('</Session>\n')
    except Exception as e:
        print e # Need to log this
        return False
    return True


def createIGVFiles(sample, logger):
    igvRecs = getATSnpIndelsForSampleIGVFiles(sample, logger)
    #s.lims_sample_id, dv.gene_id, dv.start_position, dv.variant_class_cv_id, dv.chromosome, dt.dmp_alys_task_name
    #print igvRecs
    for igvRec in igvRecs:
        sampleId = igvRec['lims_sample_id']
        geneId = igvRec['gene_id']
        startPos = igvRec['start_position']
        variantClsId = igvRec['variant_class_cv_id']
        chrom = igvRec['chromosome']
        projName = igvRec['dmp_alys_task_name']
        cvrFSLoc = "/dmp/dms/cvr-data/IMPACTG/"+projName+"/"
        print cvrFSLoc
        createIGVFile(cvrFSLoc, sampleId, geneId, startPos, chrom, projName)

    return True




if __name__=="__main__":

    parser = OptionParser(version="1.0")
    parser.add_option("-s", "--sampleName", dest="dmp_assay_lbl", help="[REQUIRED] DMP ID AKA dmp_assay_lbl in alys2sample table", default=None)
    (opts, args) = parser.parse_args()
    mSampleName = opts.dmp_assay_lbl

    mLogger = DMPLogger()
    mLogger = mLogger.getCVRLogger("dmp_cvr_germline_load")
    if not createIGVFiles(mSampleName, mLogger):
       print "FAILED TO CREATE IGV FIles"
