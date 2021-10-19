import csv
import json
import os
import argparse

"""
This script requires exporting a set of tables from HealtheIntent EDW raw tables
to CSV. There a set of mappings to RxNorm which are not available through the 
RxNorm MULTUM source and are stored in Cerner Millennium Raw Tables.

The following CSV files tables are expected:

https://sbmcin.analytics.healtheintent.com/workflows/22061#data-sets-container

RXNorm Multum

rxnorm_multum.csv

select distinct 
n.source_identifier as MULDRUG_ID,
   n.source_string as MULDRUG_DISP,
   ctm.cmti as MULDRUG_CMTI,
   --ctm.target_cmti as RXNORM_CMTI,
   --n2.source_string as RXNORM_DISP,
   n2.source_identifier as RXNORM_ID,
   n2.concept_cki as RXNORM_CCKI,
   cc.concept_cki as XMAP_CCKI,
   cc.concept_identifier as XMAP_RXCUI
from
   sbmcin_p159.nomenclature n
inner join sbmcin_p159.cmt_term_map ctm
  on n.cmti = ctm.cmti
inner join sbmcin_p159.code_value cv1
  on n.source_vocabulary_cd = cv1.code_value 
  and cv1.code_set = 400 
  and cv1.cdf_meaning = 'MULTUM'
   --and n.source_identifier = '12101'
   and n.primary_vterm_ind = 1
   and n.end_effective_dt_tm > sysdate
inner join sbmcin_p159.nomenclature n2
  on n2.cmti = ctm.target_cmti  
inner join sbmcin_p159.code_value cv2  
  on n2.source_vocabulary_cd = cv2.code_value
  and cv2.code_set = 400 
  and cv2.cdf_meaning = 'RXNORM'
inner join sbmcin_p159.cmt_concept cc
   on n2.concept_cki = cc.concept_cki
order by n.source_identifier;

rxnorm_multum_drug.csv

select distinct 
n.source_identifier as MULDRUG_ID,
   n.source_string as MULDRUG_DISP,
   ctm.cmti as MULDRUG_CMTI,
   --ctm.target_cmti as RXNORM_CMTI,
   --n2.source_string as RXNORM_DISP,
   n2.source_identifier as RXNORM_ID,
   n2.concept_cki as RXNORM_CCKI,
   cc.concept_cki as XMAP_CCKI,
   cc.concept_identifier as XMAP_RXCUI
from
   sbmcin_p159.nomenclature n
inner join sbmcin_p159.cmt_term_map ctm
  on n.cmti = ctm.cmti
inner join sbmcin_p159.code_value cv1
  on n.source_vocabulary_cd = cv1.code_value 
  and cv1.code_set = 400 
  and cv1.cdf_meaning = 'MUL.DRUG'
   --and n.source_identifier = '12101'
   and n.primary_vterm_ind = 1
   and n.end_effective_dt_tm > sysdate
inner join sbmcin_p159.nomenclature n2
  on n2.cmti = ctm.target_cmti  
inner join sbmcin_p159.code_value cv2  
  on n2.source_vocabulary_cd = cv2.code_value
  and cv2.code_set = 400 
  and cv2.cdf_meaning = 'RXNORM'
inner join sbmcin_p159.cmt_concept cc
   on n2.concept_cki = cc.concept_cki
order by n.source_identifier;

rxnorm_multum_mmdc.csv

select distinct 
n.source_identifier as MULDRUG_ID,
   n.source_string as MULDRUG_DISP,
   ctm.cmti as MULDRUG_CMTI,
   --ctm.target_cmti as RXNORM_CMTI,
   --n2.source_string as RXNORM_DISP,
   n2.source_identifier as RXNORM_ID,
   n2.concept_cki as RXNORM_CCKI,
   cc.concept_cki as XMAP_CCKI,
   cc.concept_identifier as XMAP_RXCUI
from
   sbmcin_p159.nomenclature n
inner join sbmcin_p159.cmt_term_map ctm
  on n.cmti = ctm.cmti
inner join sbmcin_p159.code_value cv1
  on n.source_vocabulary_cd = cv1.code_value 
  and cv1.code_set = 400 
  and cv1.cdf_meaning = 'MUL.DRUG'
   --and n.source_identifier = '12101'
   and n.primary_vterm_ind = 1
   and n.end_effective_dt_tm > sysdate
inner join sbmcin_p159.nomenclature n2
  on n2.cmti = ctm.target_cmti  
inner join sbmcin_p159.code_value cv2  
  on n2.source_vocabulary_cd = cv2.code_value
  and cv2.code_set = 400 
  and cv2.cdf_meaning = 'RXNORM'
inner join sbmcin_p159.cmt_concept cc
   on n2.concept_cki = cc.concept_cki
order by n.source_identifier;

"""


def main(csv_files_list, key_field="MULDRUG_ID"):

    for csv_file in csv_files_list:

        with open(csv_file, "r", newline="") as f:
            keyed_dict = {}
            csv_dict_reader = csv.DictReader(f)
            for row_dict in csv_dict_reader:
                keyed_dict[row_dict[key_field]] = row_dict

            with open(csv_file + "." + key_field + ".json", "w") as fw:
                json.dump(keyed_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Get MULTUM RxNorm to Multum mappings")

    arg_parser_obj.add_argument("-d", "--base-healtheintent-export-directory", dest="base_directory")

    arg_obj = arg_parser_obj.parse_args()

    # These are based on raw table queries in HealtheIntent
    base_names = ["rxnorm_multum", "rxnorm_multum_drug", "rxnorm_multum_mmdc"]

    csv_files = []
    for base_name in base_names:
        csv_files += [os.path.join(arg_obj.base_directory, base_name + ".csv")]

    main(csv_files)
