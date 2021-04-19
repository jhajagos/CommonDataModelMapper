
import sqlalchemy as sa
import csv
import sqlparse
import pathlib
import os


CREATE_DATABASE_SQL = """

CREATE TABLE RXNATOMARCHIVE
(
   RXAUI             varchar(8) NOT NULL,
   AUI               varchar(10),
   STR               varchar(4000) NOT NULL,
   ARCHIVE_TIMESTAMP varchar(280) NOT NULL,
   CREATED_TIMESTAMP varchar(280) NOT NULL,
   UPDATED_TIMESTAMP varchar(280) NOT NULL,
   CODE              varchar(50),
   IS_BRAND          varchar(1),
   LAT               varchar(3),
   LAST_RELEASED     varchar(30),
   SAUI              varchar(50),
   VSAB              varchar(40),
   RXCUI             varchar(8),
   SAB               varchar(20),
   TTY               varchar(20),
   MERGED_TO_RXCUI   varchar(8),
   t varchar(1)
)
;


CREATE TABLE RXNCONSO
(
   RXCUI             varchar(8) NOT NULL,
   LAT               varchar (3) DEFAULT 'ENG' NOT NULL,
   TS                varchar (1),
   LUI               varchar(8),
   STT               varchar (3),
   SUI               varchar (8),
   ISPREF            varchar (1),
   RXAUI             varchar(8) NOT NULL,
   SAUI              varchar (50),
   SCUI              varchar (50),
   SDUI              varchar (50),
   SAB               varchar (20) NOT NULL,
   TTY               varchar (20) NOT NULL,
   CODE              varchar (50) NOT NULL,
   STR               varchar (3000) NOT NULL,
   SRL               varchar (10),
   SUPPRESS          varchar (1),
   CVF               varchar(50),
   t varchar(1)
)
;


CREATE TABLE RXNREL
(
   RXCUI1    varchar(8) ,
   RXAUI1    varchar(8),
   STYPE1    varchar(50),
   REL       varchar(4) ,
   RXCUI2    varchar(8) ,
   RXAUI2    varchar(8),
   STYPE2    varchar(50),
   RELA      varchar(100) ,
   RUI       varchar(10),
   SRUI      varchar(50),
   SAB       varchar(20) NOT NULL,
   SL        varchar(1000),
   DIR       varchar(1),
   RG        varchar(10),
   SUPPRESS  varchar(1),
   CVF       varchar(50),
   t varchar(1)
)
;


CREATE TABLE RXNSAB
(
   VCUI           varchar (8),
   RCUI           varchar (8),
   VSAB           varchar (40),
   RSAB           varchar (20) NOT NULL,
   SON            varchar (3000),
   SF             varchar (20),
   SVER           varchar (20),
   VSTART         varchar (10),
   VEND           varchar (10),
   IMETA          varchar (10),
   RMETA          varchar (10),
   SLC            varchar (1000),
   SCC            varchar (1000),
   SRL            integer,
   TFR            integer,
   CFR            integer,
   CXTY           varchar (50),
   TTYL           varchar (300),
   ATNL           varchar (1000),
   LAT            varchar (3),
   CENC           varchar (20),
   CURVER         varchar (1),
   SABIN          varchar (1),
   SSN            varchar (3000),
   SCIT           varchar (4000),
   t varchar(1)
)
;


CREATE TABLE RXNSAT
(
   RXCUI            varchar(8) ,
   LUI              varchar(8),
   SUI              varchar(8),
   RXAUI            varchar(9),
   STYPE            varchar (50),
   CODE             varchar (50),
   ATUI             varchar(11),
   SATUI            varchar (50),
   ATN              varchar (1000) NOT NULL,
   SAB              varchar (20) NOT NULL,
   ATV              varchar (4000),
   SUPPRESS         varchar (1),
   CVF              varchar (50),
   t varchar(1)
)
;


CREATE TABLE RXNSTY
(
   RXCUI          varchar(8) NOT NULL,
   TUI            varchar (4),
   STN            varchar (100),
   STY            varchar (50),
   ATUI           varchar (11),
   CVF            varchar (50),
   t varchar(1)
)
;


CREATE TABLE RXNDOC (
    DOCKEY      varchar(50) NOT NULL,
    VALUE       varchar(1000),
    TYPE        varchar(50) NOT NULL,
    EXPL        varchar(1000),
   t varchar(1)
)
;


CREATE TABLE RXNCUICHANGES
(
      RXAUI         varchar(8),
      CODE          varchar(50),
      SAB           varchar(20),
      TTY           varchar(20),
      STR           varchar(3000),
      OLD_RXCUI     varchar(8) NOT NULL,
      NEW_RXCUI     varchar(8) NOT NULL,
   t varchar(1)
)
;

CREATE TABLE RXNCUI (
 cui1 VARCHAR(8),
 ver_start VARCHAR(40),
 ver_end   VARCHAR(40),
 cardinality VARCHAR(8),
 cui2       VARCHAR(8) ,
   t varchar(1)
)
;

"""

CREATE_INDICES = """
CREATE INDEX X_RXNCONSO_STR ON RXNCONSO(STR);
CREATE INDEX X_RXNCONSO_RXCUI ON RXNCONSO(RXCUI);
CREATE INDEX X_RXNCONSO_TTY ON RXNCONSO(TTY);
CREATE INDEX X_RXNCONSO_CODE ON RXNCONSO(CODE);

CREATE INDEX X_RXNSAT_RXCUI ON RXNSAT(RXCUI);
CREATE INDEX X_RXNSAT_ATV ON RXNSAT(ATV);
CREATE INDEX X_RXNSAT_ATN ON RXNSAT(ATN);

CREATE INDEX X_RXNREL_RXCUI1 ON RXNREL(RXCUI1);
CREATE INDEX X_RXNREL_RXCUI2 ON RXNREL(RXCUI2);
CREATE INDEX X_RXNREL_RELA ON RXNREL(RELA);

CREATE INDEX X_RXNATOMARCHIVE_RXAUI ON RXNATOMARCHIVE(RXAUI);
CREATE INDEX X_RXNATOMARCHIVE_RXCUI ON RXNATOMARCHIVE(RXCUI);
CREATE INDEX X_RXNATOMARCHIVE_MERGED_TO ON RXNATOMARCHIVE(MERGED_TO_RXCUI);
"""


def load_rrf_table(rrf_table_name, metadata_obj, connection, p_rrf_directory):
    rxn_obj = metadata_obj.tables[rrf_table_name]
    rrf_file_name = rrf_table_name + ".RRF"
    with open(p_rrf_directory / rrf_file_name, mode="r", errors="ignore") as f:
        rxn_reader = csv.reader(f, delimiter="|")

        i = 0
        batch_list = []
        for row_dict in rxn_reader:
            batch_list += [row_dict]
            if i % 10000 == 0 and i > 0:
                connection.execute(
                    sa.insert(rxn_obj, batch_list)
                )
                batch_list = []
                print(f"Inserted {i}")

            i += 1

        connection.execute(sa.insert(rxn_obj, batch_list))


def main(rrf_directory, connection_string):

    p_rrf_directory = pathlib.Path(rrf_directory)

    engine = sa.create_engine(connection_string)

    with engine.connect() as connection:

        create_table_list = sqlparse.split(CREATE_DATABASE_SQL)
        for create_table in create_table_list:
            connection.execute(create_table)

        metadata_obj = sa.MetaData(connection)
        metadata_obj.reflect()

        load_rrf_table("RXNSAT", metadata_obj, connection, p_rrf_directory)
        load_rrf_table("RXNREL", metadata_obj, connection, p_rrf_directory)
        load_rrf_table("RXNCONSO", metadata_obj, connection, p_rrf_directory)

        index_list = sqlparse.split(CREATE_INDICES)
        for idx_stm in index_list:
            connection.execute(idx_stm)


if __name__ == "__main__":

    if os.path.exists("./rxnorm.db3"):
        os.remove("./rxnorm.db3")

    main("C:\\users\\janos hajagos\\data\\rxnorm\\RxNorm_full_04052021\\rrf\\", "sqlite:///rxnorm.db3")


