 LOAD DATA LOCAL INFILE '~/germline_annotation_info_fmt.txt' into table germline_annotation FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
 (transcript_id,hgvs_cDNA_annotation,hgvs_aa_annotation,translation_impact,assesment,ing_phenotype,
 ing_affected_count,ing_affected_hom_count,ing_unaffected_count,inferred_activity,inferred_compound_heterozygous)
 SET ext_annotation_workflow_id = 1 ;
 LOAD DATA LOCAL INFILE '~/ing_dm_mapping.txt' into table ing_dm_mapping  FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'(ing_dm_name,misc);

cut -f 1,2,3,4,5,6,7,8,910 -d$'\t'  ~/
select dv.* from dmp_variants dv JOIN snp_indel_variants snp on snp.dmp_variant_id = dv.dmp_variant_id where alys2sample_id =%s
insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
	tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
	 occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values(4884,61322,1,4,791,790,1,0.00126,816,703,113,0.13848,367,53,336,60,'11434/2','0.000',0,'0;0',NULL, NULL,4,1,0,0,1);
insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter) values(1,4,'TET2','exon9',106190797,NULL,'C','A',1,'NM_001127208','c.4075C>A','p.R1359S',5,'','','',0,0,1) ;
insert into assesment_cv (assesment_name,description) SELECT  distinct assesment,assesment from germline_annotation ;


normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps ,
tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median,
support_for_som_call,occurance_in_normal,occurance_in_pop,is_silent,is_panel

snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp,tumor_rd,tumor_ad,tumor_vfreq,tumor_rd_ps,tumor_ad_ps,tumor_rd_ns,tumor_ad_ns,normal_agg_ad,normal_vfreq_median,support_for_som_call,occurance_in_normal,occurance_in_pop,is_silent,is_panel,variant_status_cv_id,comments,confidence_cv_id,is_manually_loaded,alys2sample_id,dmp_variant_id
insert into assesment_cv (assesment_name,description)values('Select one','');
insert into assesment_cv (assesment_name,description)values('Pathogenic','Pathogenic');
insert into assesment_cv (assesment_name,description)values('Likely Pathogenic','Likely Pathogenic');
insert into assesment_cv (assesment_name,description)values('Variant of Unknown Significance (VUS)','Variant of Unknown Significance (VUS)');
insert into assesment_cv (assesment_name,description)values('Likely Benign','Likely Benign')
insert into assesment_cv (assesment_name,description)values('Benign','Benign');

LOAD DATA LOCAL INFILE '~/output.txt' into table dmp_sample FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (dmp_sample_id,m_accession_num,lims_sample_id,dmp_patient_id,is_tumor,is_clinical,tumor_type_cv_id,counter,tumor_purity,is_metastasis,primary_site,metastasis_site,dmp_sample_lbl,legacy_sample_lbl);


 alter table msk_gml_annotation ADD ExAC varchar(20) after ESP ;
 alter table msk_gml_annotation ADD ExAC_AFR varchar(20) after ExAC;
 alter table msk_gml_annotation ADD ExAC_AMR varchar(20) after ExAC_AFR
 alter table msk_gml_annotation ADD ExAC_EAS  varchar(20) after ExAC_AMR  ;
 alter table msk_gml_annotation ADD ExAC_FIN   varchar(20) after ExAC_EAS ;
 alter table msk_gml_annotation ADD ExAC_NFE varchar(20) after ExAC_FIN ;
 alter table msk_gml_annotation ADD ExAC_OTH  varchar(20) after ExAC_NFE ;
 alter table msk_gml_annotation ADD ExAC_SAS  varchar(20) after ExAC_OTH ;

 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,20528,1,4,596,595,0,0,710,363,347,0.48873,181,154,182,193,'8035/0','0',0,'0;0',NULL, NULL,4,1,0,0,1);



 insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,
 	cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,11,'CCND1','exon1',69456211,NULL,'T','G',0,'NM_053056','c.130T>G','p.Y44D',1,'','','',0,0,1) ;


 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,87648,1,4,807,806,0,0,1045,663,400,0.38278,317,177,326,223,'10273/0','0',0,'0;0',NULL, NULL,4,1,0,0,1);


  insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,
 	cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,11,'ATM','exon35',108172386,NULL,'G','A',0,'NM_000051','c.5189G>A','p.R1730Q',1,'','','',0,0,1) ;

 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,87649,1,4,586,586,0,0,556,298,255,0.45863,175,157,123,98,'12722/11','0',0,'0;0',NULL, NULL,4,1,0,0,1);

 insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,
 	cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,13,'ERCC5','exon10',103518624,NULL,'A','G',0,'NM_000123','c.2212A>G','p.T738A',1,'','','',0,0,1) ;


  insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,87650,1,4,626,625,0,0,760,402,355,0.46711,252,236,150,119,'14104/0','0',0,'0;0',NULL, NULL,4,1,0,0,1);

 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,87650,1,4,626,625,0,0,760,402,355,0.46711,252,236,150,119,'14104/0','0',0,'0;0',NULL, NULL,4,1,0,0,1);

  insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,
 	cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,13,'','exon11',32912095,NULL,'C','A',0,'NM_000059','c.3603C>A','p.N1201K',1,'','','',0,0,1) ;

87652

 variant_class_cv_id | variant_class        | description                                                                                       |
+---------------------+----------------------+---------------------------------------------------------------------------------------------------+
|                   1 | nonsynonymous_SNV    | Due to the SNV a missense or a nonsense mutation induced an altered aminoacid during translation  |
|                   2 | synonymous_SNV       | Due to the SNV a silent mutation occurred which has no functional affect on the coded aminoacid   |
|                   3 | frameshift_deletion  | Number of DNA, not divisible by three, have been deleted and resulted in a shift in coding frame  |
|                   4 | frameshift_insertion | Number of DNA, not divisible by three, have been inserted and resulted in a shift in coding frame |
|                   5 | stopgain_SNV         | Variant causes a stop codon to be created at the variant site                                     |



  insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,87652,1,4,1055,1053,2,0.0019,1002,664,338,0.33733,331,165,333,173,'14227/9','0',0,'0;0',NULL, NULL,4,1,0,0,1);

 312	0	0.0	960	707	0.73646	0;0

insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6072,85253,1,4,312,'',2,0.0,960,'',707,0.73646,331,165,333,173,'14227/9','0',0,'0;0',NULL, NULL,4,1,0,0,1);

  insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,
 	cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,4,'TEM','exon11',106197148,NULL,'GC','G',1,'NM_001127208','c.5482delC','p.Q1828fs',3,'','','',0,0,1) ; #97413

 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6372,97413,1,4,826,825,0,0,533,454,79,0.14822,236,40,218,39,'12580/0','0','','0;0',NULL, NULL,4,1,0,0,1);

 insert into dmp_variants(vdb_version_id,chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,cDNA_change,aa_change,variant_class_cv_id,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted,hit_counter)
 	values(1,2,'DNMT3A','exon23',25457242,NULL,'C','T',0,'NM_022552','c.2645G>A','p.R882H',1,'rs147001633','ID=COSM52944,COSM442676;OCCURENCE=1(breast),386(haematopoietic_and_lymphoid_tissue)','',1,0,1)  #6209



 insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6372,6209,1,4,703,701,0,0,658,497,161,0.24468,229,76,285,85,'12873/22','0.001','','0;0',NULL, NULL,4,1,0,0,1);

 4,TET2,exon11,106197148,",GC,G,1,NM_001127208,c.5482delC,p.Q1828fs,frameshift_deletion,' ',0,0

 2/DNMT3A/exon23/25457242/"/C/T/0/NM_022552/c.2645G>A/p.R882H/nonsynonymous_SNV/rs147001633/ID=COSM52944,COSM442676;OCCURENCE=1(breast),386(haematopoietic_and_lymphoid_tissue)/"/1/0
 2,DNMT3A,exon23,25457242,",C,T,0,NM_022552,c.2645G>A,p.R882H,nonsynonymous_SNV,rs147001633,ID=COSM52944,COSM442676;OCCURENCE=1(breast)386(haematopoietic_and_lymphoid_tissue),",1,0
 chromosome,gene_id,exon_num,start_position,stop_position,ref_allele,alt_allele,is_indel,transcript_id,cDNA_change,aa_change,variant_class,dbSNP_id,cosmic_id,mafreq_1000g,is_hotspot,is_blacklisted "


insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6767,41153,1,13,721,720,0,0,767,750,17,0.02216,403,11,347,6,'13660/0','0.000','','0;0',NULL, NULL,4,1,0,0,0);


insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (6879,6209,1,13,1094,1091,1,0.00091,448,410,38,0.08482,191,219,19,19,'13264/23','0.001','','0;0',NULL, NULL,4,1,0,0,0);



insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (7023,19318,1,13,151,151,0,0,138,121,17,0.12319,63,11,58,6,'11267/0','0','','0;0',NULL, NULL,4,1,0,0,1);


insert into snp_indel_variants(alys2sample_id,dmp_variant_id, snp_indel_tool_cv_id,snp_indel_filter_cv_id,normal_dp,normal_rd,normal_ad,normal_vfreq,tumor_dp ,
tumor_rd ,tumor_ad,tumor_vfreq ,tumor_rd_ps,tumor_ad_ps , tumor_rd_ns,tumor_ad_ns ,normal_agg_ad,normal_vfreq_median, support_for_som_call,occurance_in_normal,
occurance_in_pop,comments,confidence_cv_id,variant_status_cv_id,is_manually_loaded,is_silent,is_panel)values
 (7304,110185,1,13,493,493,0,0,767,686,79,0.103,358,328,40,39,'16671/13','0','','0;0',NULL, NULL,4,1,0,0,1);


  dmp_variant_id | snp_indel_tool_cv_id | snp_indel_filter_cv_id | normal_dp | normal_rd | normal_ad | normal_vfreq | tumor_dp | tumor_rd | tumor_ad | tumor_vfreq | tumor_rd_ps | tumor_ad_ps | tumor_rd_ns | tumor_ad_ns | normal_agg_ad | normal_vfreq_median | support_for_som_call | occurance_in_normal | occurance_in_pop | comments                        | confidence_cv_id | variant_status_cv_id | is_manually_loaded | is_silent | is_panel

 6767 |          40793 |                    1 |                     13 |       721 |       720 |         0 |            0 |      767 |      750 |       17 |     0.02216 |         403 |          11 |         347 |           6 | 13660/0       | 0.000               | 0                    | 0;0                 | NULL             | needs to be fixed, update Aijaz |                4 |                    1 |                  0 |         0 |        1 | 2015-07-23 09:24:21 |