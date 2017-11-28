Mapping to OMOP CDM

There is two step process to transform source CSV files into the OMOP/OHDSI 
common data model compatible CSV files. The first step is mapping the source file into the 
prepared source file format. Examples of files in the prepared source format 
can be seen in the `./test/input/` directory.

If files are formatted into prepared source then the script 
`transform_prepared_source_to_cdm.py` can be run to convert the input files 
into output files.

