import sqlalchemy as sa
import pandas as pd
import json
import os
import argparse


def main(directory, connection_uri):

    engine = sa.create_engine(connection_uri)

    with engine.connect() as connection:

        q1 = """
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
        """

        q1_df = pd.read_sql(q1, connection)
        q1_df_csv_file_name = os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")
        q1_df.to_csv(q1_df_csv_file_name, index=False)

        q2 = """
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

        q2_df = pd.read_sql(q2, connection)
        q2_df_csv_file_name = os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")
        q2_df.to_csv(q2_df_csv_file_name, index=False)


        q3 = """
        select t.* from (
                         select distinct r1.RXCUI,
                                         r1.TTY,
                                         r1.STR,
                                         r2.RXCUI as IN_RXCUI,
                                         r2.TTY   as IN_TTY,
                                         r2.STR   as IN_STR
                         from RXNCONSO r1
                                  join RXNREL rr on r1.RXCUI = rr.RXCUI1
                                  join RXNCONSO r2 on r2.RXCUI = rr.RXCUI2
                         where r1.SAB = 'RXNORM'
                           and r2.SAB = 'RXNORM'
                           and r1.SUPPRESS = 'N'
                           and r1.TTY = 'BN'
                           and r2.TTY in ('IN')

                     ) t
    join RXNSAT rs on rs.rxcui = t.rxcui and rs.sab = 'RXNORM' and rs.atn = 'RXN_BN_CARDINALITY' and rs.ATV = 'single'
order by cast(t.RXCUI as int), TTY, RXCUI
        
        """

        q3_df = pd.read_sql(q3, connection)
        q3_df_csv_file_name = os.path.join(directory, "select_bn_single_in.csv")
        q3_df.to_csv(q3_df_csv_file_name, index=False)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Generate brand name mappings")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="./rxnorm.json")
    arg_parser_obj.add_argument("-f", "--path-to-sqlite-file-name", dest="path_to_sqlite_file_name",
                               default="./rxnorm.db3")

    arg_obj = arg_parser_obj.parse_args()
    config_json_file_name = arg_obj.config_json_file_name

    with open(config_json_file_name, "r") as f:
         config = json.load(f)

    destination_directory = config["json_map_directory"]
    connection_uri = "sqlite:///" + arg_obj.path_to_sqlite_file_name

    main(destination_directory, connection_uri)
