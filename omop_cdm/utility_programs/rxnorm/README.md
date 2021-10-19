## Generate additional mappings for RxNorm

### Download RxNorm RRF Files from UML

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
python3 generate_brand_name_to_csv_mappings.py -c ./rxnorm.json -f ./rxnorm.db3
```

This will generate a set of CSV files. Once the CSV files
is generated we need to concert the files to a JSON mapping file.

```bash
python3 create_brand_name_to_standard_mapping.py -c ./rxnorm.json -f ./rxnorm.db3
```

### Build mappings extracted from Millennium Raw Tables

We rely on the mappings from Cerner Millennium raw tables
for RxNorm mappings for code categories not in the standard
RxNorm source. This requires generating and exporting tables
from HealtheIntent.

```bash
python3 ./multum_sourced_rxnorm_mappings.py -f /data/rxnorm/healtheintent/
```

### Build mappings from RxNorm Sourced Mappings

```bash
python3 ./rxnorm_sourced_multum_mappings.py -c ./rxnorm.json
```