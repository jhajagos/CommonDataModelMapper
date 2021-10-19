## Generate additional mappings for RxNorm

### Download RxNorm RRF Files from UMLS

You can download the latest release of RxNorm from here:

https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html


### Building a SQLite RxNorm Database

The first step is to load the RRF files into a SQLite
database.

```bash
python3 ./load_rxnorm_to_sqlite3.py -f /data/rxnorm/RxNorm_full_04052021/rrf/ -d ./rxnorm.db3
```

### Brand name mappings

First we need to define `./rxnorm.json` with two keys:

```json
{
  "json_map_directory": "/data/ohdsi/vocabulary/",
  "rxnorm_base_directory": "/data/rxnorm/RxNorm_full_04052021/rrf/"
}
```

We utilize the SQLite database created in the previous step to 
map brand names to RxNorm concepts.

```bash
python generate_brand_name_to_csv_mappings.py -c ./rxnorm.json -f ./rxnorm.db3
```

This will generate a set of CSV files. Once the CSV files
is generated we need to concert the files to a JSON mapping file.

```bash
python create_brand_name_to_standard_mapping.py -c ./rxnorm.json -f ./rxnorm.db3
```

### Build mappings extracted from Millennium Raw Tables

We rely on the mappings from Cerner Millennium raw tables
for RxNorm mappings for code categories not in the standard
RxNorm source. This requires generating and exporting tables
from HealtheIntent using the workflow [RXNorm Multum](
https://sbmcin.analytics.healtheintent.com/workflows/22061#data-sets-container
).  The following CSV files tables are expected: 
`rxnorm_multum.csv`, `rxnorm_multum_drug.csv`, `rxnorm_multum_mmdc.csv`.


```bash
python3 ./multum_sourced_rxnorm_mappings.py -f /data/rxnorm/healtheintent/
```


### Build mappings from RxNorm Sourced Mappings

```bash
python3 ./rxnorm_sourced_multum_mappings.py -c ./rxnorm.json
```

#### Upload files to Vocab

The following files should have been generated

```
RxNorm_MMSL_BD.json
RxNorm_MMSL_BN.json
RxNorm_MMSL_CD.json
RxNorm_MMSL_GN.json
rxnorm_multum.csv.MULDRUG_ID.json
rxnorm_multum_drug.csv.MULDRUG_ID.json
rxnorm_multum_mmdc.csv.MULDRUG_ID.json
select_bn_single_in.csv.RXCUI.json
select_bn_single_in.csv.STR.json
select_n_in__ot___from___select_bn_rxcui.csv.bn_rxcui.json
select_n_in__ot___from___select_bn_rxcui.csv.bn_str.json
select_tt_n_sbdf__ott___from___select_bn.csv.bn_rxcui.json
select_tt_n_sbdf__ott___from___select_bn.csv.bn_str.json
```