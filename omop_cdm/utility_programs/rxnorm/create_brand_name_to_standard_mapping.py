import json
import csv
import os

from multum_sourced_rxnorm_mappings import main as convert_csv_to_json

def main(directory):

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_rxcui")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_rxcui")

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_str")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_str")


if __name__ == "__main__":
    base_dir = "E:\\data\\rxnorm_in_sbdf\\"
    main(base_dir)




"""
select tt.n_sbdf, ott.* from (
select bn_str, bn_rxcui, count(*) as n_sbdf from (
select distinct tr1.str as bn_str, tr1.rxcui as bn_rxcui, tr1.TTY as bn_tty, tr3.STR as sbdf_str, tr3.rxcui as sbdf_rxcui , tr3.TTY as sbdf_tty
from (
select * from rxnconso rx1 where rx1.tty = 'BN' and rx1.SAB = 'RXNORM') tr1
join rxnrel r1 on r1.RXCUI1 = tr1.RXCUI and tr1.SAB = 'RXNORM'
join rxnconso tr2 on tr2.rxcui = r1.RXCUI2 and tr2.SAB = 'RXNORM' and tr2.TTY = 'SBDG'
join rxnrel r2 on tr2.RXCUI =  r2.RXCUI1 and r2.SAB = 'RXNORM'
join rxnconso tr3 on r2.RXCUI2 = tr3.RXCUI and tr3.SAB = 'RXNORM' and tr3.TTY = 'SBDF'
order by tr1.str, tr3.str) t group by bn_str, bn_rxcui) tt
join (
select distinct tr1.str as bn_str, tr1.rxcui as bn_rxcui, tr1.TTY as bn_tty, tr3.STR as sbdf_str, tr3.rxcui as sbdf_rxcui , tr3.TTY as sbdf_tty
from (
select * from rxnconso rx1 where rx1.tty = 'BN' and rx1.SAB = 'RXNORM') tr1
join rxnrel r1 on r1.RXCUI1 = tr1.RXCUI and tr1.SAB = 'RXNORM'
join rxnconso tr2 on tr2.rxcui = r1.RXCUI2 and tr2.SAB = 'RXNORM' and tr2.TTY = 'SBDG'
join rxnrel r2 on tr2.RXCUI =  r2.RXCUI1 and r2.SAB = 'RXNORM'
join rxnconso tr3 on r2.RXCUI2 = tr3.RXCUI and tr3.SAB = 'RXNORM' and tr3.TTY = 'SBDF') ott on tt.bn_rxcui = ott.bn_rxcui
where n_sbdf = 1
order by bn_str, sbdf_str
;


select n_in, ot.* from (
select bn_rxcui, count(*) as n_in from (
select distinct tr1.str as bn_str, tr1.rxcui as bn_rxcui, tr1.TTY as bn_tty,
  tr2.str as in_str, tr2.RXCUI as in_rxcui
from (
select * from rxnconso rx1 where rx1.tty = 'BN' and rx1.SAB = 'RXNORM') tr1
join rxnrel r1 on r1.RXCUI1 = tr1.RXCUI  and r1.SAB = 'RXNORM'
join rxnconso tr2 on r1.RXCUI2 = tr2.rxcui and tr2.SAB = 'RXNORM' and tr2.TTY = 'IN') t group by bn_rxcui) tt
join
  (select distinct tr1.str as bn_str, tr1.rxcui as bn_rxcui, tr1.TTY as bn_tty,
  tr2.str as in_str, tr2.RXCUI as in_rxcui
from (
select * from rxnconso rx1 where rx1.tty = 'BN' and rx1.SAB = 'RXNORM') tr1
join rxnrel r1 on r1.RXCUI1 = tr1.RXCUI  and r1.SAB = 'RXNORM'
join rxnconso tr2 on r1.RXCUI2 = tr2.rxcui and tr2.SAB = 'RXNORM' and tr2.TTY = 'IN') ot on ot.bn_rxcui = tt.bn_rxcui
  where n_in = 1
order by bn_str, in_str;
"""