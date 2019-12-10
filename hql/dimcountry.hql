-- 
-- DESC	Glu Title Metadata
-- AUTH	Justin the Smith
-- DATE	2019-11-21
-- DEST	s3://glu-emr/tables/utility.db/dim_country
-- DEST	s3://glu-emr/tables/utility.db/dimcountry_text
-- DATA	s3://glu-emr/utility/dimcountry/
--

-- configure hive
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.compress.output=true;
set hive.execution.engine=tez;
set hive.exec.stagingdir=/tmp/hive/;
set hive.exec.scratchdir=/tmp/hive/;
set hive.tez.auto.reducer.parallelism = true;
set hive.merge.tezfiles=true;
set mapreduce.input.fileinputformat.split.maxsize=5000000;

-- clean up old mess
DROP TABLE IF EXISTS
	utility.dimcountry_text
;

-- create storage for output
CREATE EXTERNAL TABLE IF NOT EXISTS
	utility.dimcountry_text
	(	glu_country	STRING
	,	un_country	STRING
	,	un_region	STRING
	,	iso2	STRING
	,	iso3	STRING
	)
	ROW FORMAT
		SERDE	'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED
		AS TEXTFILE
    LOCATION
    	's3://glu-emr/utility/dimcountry/'
;

DROP TABLE IF EXISTS
	utility.dim_country
;

-- create storage for output
CREATE EXTERNAL TABLE IF NOT EXISTS
	utility.dim_country
	(	glu_country	STRING
	,	un_country	STRING
	,	un_region	STRING
	,	iso2	STRING
	,	iso3	STRING
	)
	STORED
		AS orc
	LOCATION
		's3://glu-emr/tables/utility.db/dimcountry'
;

-- migrate to orc
INSERT
	OVERWRITE
TABLE
	utility.dim_country
SELECT
	*
FROM
	utility.dimcountry_text
;

-- clean up old mess
DROP TABLE IF EXISTS
	utility.dimcountry_text
;

-- compute table stats for query optimizer
ANALYZE
TABLE
	utility.dim_country
COMPUTE
	STATISTICS
FOR	COLUMNS
;
