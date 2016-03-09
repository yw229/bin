select
           dp.dmp_patient_lbl AS PATIENT_ID ,
            ds.dmp_sample_lbl AS SAMPLE_ID ,
            at.dmp_alys_task_name,
            dp.mrn
        from
            dmp_alys_task at
        left join
            dmp_dms dms
        on (at.dmp_dms_id=dms.dmp_dms_id)
        left join
            alys2sample a2s
        on (at.dmp_alys_task_id=a2s.dmp_alys_task_id)
        left join
            dmp_sample ds
        on (a2s.dmp_sample_id=ds.dmp_sample_id)
        left join
            dmp_patient dp
        on (ds.dmp_patient_id=dp.dmp_patient_id)
        where ds.dmp_sample_lbl = 'P-0001215-T01'
        order by at.dmp_alys_task_id