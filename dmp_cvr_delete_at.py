#!/dmp/resources/prod/tools/sys'em/python/Python-2.7.3/bin/python
# -*- coding: utf-8 -*-
import os
import sys 
import argparse
from mskcc.dmp.cvr.utils.dmp_logger import DMPLogger
from mskcc.dmp.cvr.utils.dmp_properties import Properties
from org.mskcc.dmp.dmutils.dmp_db import DMPdb


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
        raise("Database handle not created!")
    return dbObj

'''
    Given a list, print out each ele in tab-delimited formate.
'''
def print_formatted_columns(l):
    print '     '.join('{}'.format(el) for el in l)

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
'''
    dict to map each unique record and it's counter infor based on the given filed 
'''
def sql_counter_dic(dbObj,sql,f,f_counter):
    counter_dic = {}
    dbObj.execute(sql) #bool
    rec = dbObj.fetchall() #tuple
    for r in rec:
        counter_dic[r[f]] = r[f_counter]     
    
    return counter_dic 
'''
    Given a analysis_task name or id, query all alys2sample_id from alys2sample ; 
'''
def update_counter(alyst):
    logger = DMPLogger()
    logger = logger.getCVRLogger("TestDB")
    logger.debug("Creating database connection")
    dbObj = None
    try:
        dbObj = getCVRDBHandle(logger)
        dbObj.connect()
        logger.debug("Successful connection")
        
        sql_a2s_t_info = "select a2s.*,t.* from dmp_alys_task t JOIN alys2sample a2s ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name ='%s'" 

        sql_sample = "select DISTINCT s.*,count(t.dmp_alys_task_id) AS task_counter from dmp_sample s JOIN alys2sample a2s ON s.dmp_sample_id =a2s.dmp_sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s' group by s.dmp_sample_id "
        sql_pi = "select  DISTINCT p.* ,count(s.dmp_sample_id) AS s_counter from dmp_sample s JOIN alys2sample a2s ON s.dmp_sample_id =a2s.dmp_sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_patient p ON s.dmp_patient_id = p.dmp_patient_id where t.dmp_alys_task_name ='%s' group by p.dmp_patient_id "
        sql_dmpv ="select DISTINCT dmpv.*,count(snp.snp_indel_variant_id) AS snp_counter  from alys2sample a2s JOIN snp_indel_variants snp ON a2s.alys2sample_id =snp.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_variants dmpv ON snp.dmp_variant_id = dmpv.dmp_variant_id where t.dmp_alys_task_name = '%s' group by snp.dmp_variant_id "

        sql_dmp_dms = "select DISTINCT dms.* from  alys2sample a2s JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_dms dms ON dms.dmp_dms_id =t.dmp_dms_id  where t.dmp_alys_task_name = '%s'"
        sql_dmp_lims = "select DISTINCT lims.* from  alys2sample a2s JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_lims lims ON lims.dmp_lims_id =t.dmp_lims_id  where t.dmp_alys_task_name = '%s'" 
        sql_params = "select DISTINCT params.* from  alys2sample a2s JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN a2s_params params  ON a2s.alys2sample_id =params.alys2sample_id  where t.dmp_alys_task_name = '%s'"
        sql_mrev = "select DISTINCT mre.* from dmp_sample_mrev mre JOIN alys2sample a2s ON mre.alys2sample_id = a2s.alys2sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id WHERE t.dmp_alys_task_name = '%s'" 
        sql_cnv = "select DISTINCT cnv.* from alys2sample a2s JOIN cnv_variants cnv ON a2s.alys2sample_id = cnv.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s'" 
        sql_snp = " select DISTINCT snp.* from alys2sample a2s JOIN snp_indel_variants snp ON a2s.alys2sample_id = snp.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_variants dmpv ON snp.dmp_variant_id = dmpv.dmp_variant_id where t.dmp_alys_task_name = '%s' order by snp.dmp_variant_id "
        
        sql_intrav = "select DISTINCT intrav.* from cnv_intragenic_variants intrav JOIN alys2sample a2s ON a2s.alys2sample_id = intrav.alys2sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id where t.dmp_alys_task_name = '%s'" 
        sql_sv ="select DISTINCT sv.* from alys2sample a2s JOIN structural_variants sv ON a2s.alys2sample_id = sv.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s'"
        
        #rollback the counter for dmp_sample.dmp_patient and dmp_variants 
        rb_pi ='UPDATE dmp_patient p JOIN (select p.dmp_patient_id ,s.dmp_sample_id,p.counter,count(s.dmp_sample_id) AS sample_counter from dmp_patient p JOIN dmp_sample s on s.dmp_patient_id = p.dmp_patient_id JOIN alys2sample a2s on s.dmp_sample_id = a2s.dmp_sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id group by p.dmp_patient_id having sample_counter != p.counter) r ON p.dmp_patient_id = r.dmp_patient_id SET p.counter = r.sample_counter ' 
        rb_sample = 'UPDATE dmp_sample s JOIN (select s.dmp_sample_id, t.dmp_alys_task_id, s.counter, count(t.dmp_alys_task_id) AS task_counter from dmp_sample s JOIN alys2sample a2s ON s.dmp_sample_id =a2s.dmp_sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id group by s.dmp_sample_id having s.counter != task_counter ) r ON s.dmp_sample_id = r.dmp_sample_id SET s.counter = r.task_counter '
        rb_dmpv = 'UPDATE dmp_variants v JOIN (select snp.dmp_variant_id ,count(snp.snp_indel_variant_id) AS snp_counter ,v.hit_counter from snp_indel_variants snp JOIN alys2sample a2s ON a2s.alys2sample_id = snp.alys2sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id JOIN dmp_sample s ON s.dmp_sample_id = a2s.dmp_sample_id JOIN dmp_variants v ON v.dmp_variant_id = snp.dmp_variant_id group by snp.dmp_variant_id having snp_counter !=v.hit_counter ) r ON v.dmp_variant_id = r.dmp_variant_id SET v.hit_counter = r.snp_counter' 
        
        sql_cursor_exe(dbObj,sql_a2s_t_info%alyst)

        #print all dmp_dms info 
        sql_cursor_exe(dbObj,sql_dmp_dms%alyst)

        #print all dmp_lims info
        sql_cursor_exe(dbObj,sql_dmp_lims%alyst)

        #print all a2s_params info 
        sql_cursor_exe(dbObj,sql_params%alyst)

        #print all dmp_sample_mrev info 
        sql_cursor_exe(dbObj,sql_mrev%alyst)

        #print all structural_variants info 
        sql_cursor_exe(dbObj,sql_sv%alyst)
        
        #print all cnv_variants info 
        sql_cursor_exe(dbObj,sql_cnv%alyst)
        
        #print all snp_variants info
        sql_cursor_exe(dbObj,sql_snp%alyst)
        
        #print all snp_indel_variants info 
        sql_cursor_exe(dbObj,sql_snp%alyst)
        
        #print all cnv_tragenic_variants info 
        sql_cursor_exe(dbObj,sql_intrav%alyst)
        
        #print dmp_variants_id and total_counter for snp_indel_variant under it 
        sql_cursor_exe(dbObj,sql_dmpv%alyst)
        # print the column names
        
        #rollback the counter for sample,pi and dmp_variant
        dbObj.execute(rb_pi)
        dbObj.execute(rb_sample)
        dbObj.execute(rb_dmpv)
        
        print 'OK'
    except Exception as e :
        logger.error(e)
    finally:
        if dbObj:
            dbObj.disconnect()
            
    logger.debug("Completed testing!")

def deleteTask(alyst): 
    #Procedures to remove all associated records when given a task name: 
    #1.Never DELETE records in dmp_sample,dmp_patient,dmp_variants tables, only update the counter fields by calling update_counter(alyst) method 
    logger = DMPLogger()
    logger = logger.getCVRLogger("TestDB")
    logger.debug("Creating database connection")
    dbObj = None
    try:
        dbObj = getCVRDBHandle(logger)
        dbObj.connect()
        logger.debug("Successful connection")
        
        sql_task = "select * from dmp_alys_task where dmp_alys_task_name = '%s'"
        sql_sample = "select DISTINCT s.*,count(t.dmp_alys_task_id) AS task_counter from dmp_sample s JOIN alys2sample a2s ON s.dmp_sample_id =a2s.dmp_sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s' group by s.dmp_sample_id "
        sql_pi = "select  DISTINCT p.* ,count(s.dmp_sample_id) AS s_counter from dmp_sample s JOIN alys2sample a2s ON s.dmp_sample_id =a2s.dmp_sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_patient p ON s.dmp_patient_id = p.dmp_patient_id where t.dmp_alys_task_name ='%s' group by p.dmp_patient_id "
        sql_dmpv ="select DISTINCT dmpv.*,count(snp.snp_indel_variant_id) AS snp_counter  from alys2sample a2s JOIN snp_indel_variants snp ON a2s.alys2sample_id =snp.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_variants dmpv ON snp.dmp_variant_id = dmpv.dmp_variant_id where t.dmp_alys_task_name = '%s' group by snp.dmp_variant_id "
        
        #update the counter in dmp_sample,dmp_patient,dmp_variants 
        sql_update_dmp_sample_counter = "UPDATE dmp_sample s SET s.counter= s.counter - %s WHERE s.dmp_sample_id = %s"

        sql_update_dmp_patient_counter = "UPDATE dmp_patient p SET p.counter = p.counter- %s WHERE p.dmp_patient_id = %s"

        sql_update_dmpv_counter = "UPDATE  dmp_variants v SET v.hit_counter = v.hit_counter -%s WHERE v.dmp_variant_id = %s"

        dbObj.execute(sql_task%alyst)
        rec = dbObj.fetchall() #tuple
        #validate the task exists in database
        if not rec:
            print "Task %s does not exist in table" %alyst
        else:    
            #print dmp_sample_id and total_counter for dmp_alys_task under it 
            dmps_dic = sql_counter_dic(dbObj,sql_sample%alyst,'dmp_sample_id','task_counter')
            for k in dmps_dic:
                dbObj.execute(sql_update_dmp_sample_counter%(dmps_dic[k],k))

            #print dmp_patient_id and total_sample for samples under each patient 
            sql_cursor_exe(dbObj,sql_pi%alyst)
            dmpp_dic = sql_counter_dic(dbObj,sql_pi%alyst,'dmp_patient_id','s_counter')
            for k in dmpp_dic:
                dbObj.execute(sql_update_dmp_patient_counter%(dmpp_dic[k],k))
        
             #print dmp_variant_id and total_snp_idel_counter under each variant 
            sql_cursor_exe(dbObj,sql_dmpv%alyst)
            dmps_dic = sql_counter_dic(dbObj,sql_sample%alyst,'dmp_sample_id','task_counter')
            for k in dmps_dic:
                dbObj.execute(sql_update_dmp_sample_counter%(dmps_dic[k],k))

            print 'Counter updated Done !'
       
            del_snp = "DELETE snp.* from alys2sample a2s JOIN snp_indel_variants snp ON a2s.alys2sample_id = snp.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_variants dmpv ON snp.dmp_variant_id = dmpv.dmp_variant_id where t.dmp_alys_task_name = '%s'" 
        
            del_dmp_dms = "DELETE dms.* from  alys2sample a2s JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN dmp_dms dms ON dms.dmp_dms_id =t.dmp_dms_id  where t.dmp_alys_task_name = '%s'"
            del_params = "DELETE params.* from  alys2sample a2s JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id JOIN a2s_params params  ON a2s.alys2sample_id =params.alys2sample_id  where t.dmp_alys_task_name = '%s'"
            del_mrev = "DELETE mre.* from dmp_sample_mrev mre JOIN alys2sample a2s ON mre.alys2sample_id = a2s.alys2sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id WHERE t.dmp_alys_task_name = '%s'"
            del_cnv = "DELETE cnv.* from alys2sample a2s JOIN cnv_variants cnv ON a2s.alys2sample_id = cnv.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s'"
            del_intrav = "DELETE intrav.* from cnv_intragenic_variants intrav JOIN alys2sample a2s ON a2s.alys2sample_id = intrav.alys2sample_id JOIN dmp_alys_task t ON t.dmp_alys_task_id = a2s.dmp_alys_task_id where t.dmp_alys_task_name = '%s'"
            del_sv ="DELETE sv.* from alys2sample a2s JOIN structural_variants sv ON a2s.alys2sample_id = sv.alys2sample_id JOIN dmp_alys_task t ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name = '%s'"
            del_a2s = "DELETE a2s.* from dmp_alys_task t JOIN alys2sample a2s ON a2s.dmp_alys_task_id = t.dmp_alys_task_id where t.dmp_alys_task_name ='%s'"
            del_task = "DELETE t.* from dmp_alys_task t WHERE t.dmp_alys_task_name = '%s'"
       
        
            #2.Remove all related records in a2s_params,dmp_sample_mrev  
            dbObj.execute(del_params%alyst)
            dbObj.execute(del_mrev%alyst) 
            #3.After counter records have been updated,delete all associated records in variants tables
            dbObj.execute(del_snp%alyst)
            dbObj.execute(del_cnv%alyst)
            dbObj.execute(del_intrav%alyst)
            dbObj.execute(del_sv%alyst)
            #4.Then remove records in alys2sample
            dbObj.execute(del_a2s%alyst)
            #5.Remove records in dmp_alys_task
            dbObj.execute(del_task%alyst)
            #. Finally remove records in dmp_dms 
            dbObj.execute(del_dmp_dms%alyst) 
            print 'All delete task has been done !'
    
    except Exception as e :
            logger.error(e)
    finally:
           if dbObj:
                dbObj.disconnect()
    logger.debug("Completed testing!")

def main(): # Setting up argparse,accepting user input arguments to generate command line
    parser = argparse.ArgumentParser(description=__doc__,
                                 epilog="""If you have any questions,please
                                 contact wangy6@mskcc.org""")
    
    parser.add_argument('-t','--analysis_task_name',action='store', dest='alyst',
                    required=True, help='***Find all the task information*** \n'+
                                'Please give the analysis task name that needs to be DELETEd\n')
    
    user_args = parser.parse_args()
    alyst = user_args.alyst
    deleteTask(alyst)
    #update_counter(alyst)
 
if __name__=="__main__":
    main() 
    #update_counter('IMPACTv3-CLIN-20140156')
