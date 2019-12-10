-- 
-- DESC	Glu Title Metadata
-- AUTH	Justin the Smith
-- DATE	2019-12-10
-- DEST	s3://glu-emr/tables/utility.db/dimtitle
-- DEST	s3://glu-emr/tables/utility.db/dimtitle_text
-- DATA	s3://glu-emr/utility/dimtitle/
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
	utility.dimtitle_text
;

-- create storage for output
CREATE EXTERNAL TABLE IF NOT EXISTS
	utility.dimtitle_text
	(	public_name	STRING
	,	abbreviation	STRING
	,	game	STRING
	,	release	INTEGER
	,	game_base	STRING
	,	platform	STRING
	,	environment	STRING
	,	game_name	STRING
	,	release_date	DATE
	,	orig_release_date	DATE
	)
	ROW FORMAT
		DELIMITED
    FIELDS
    	TERMINATED BY ','
    STORED
		AS TEXTFILE
    LOCATION
    	's3://glu-emr/utility/dimtitle/'
;

DROP TABLE IF EXISTS
	utility.dim_title
;

-- create storage for output
CREATE EXTERNAL TABLE IF NOT EXISTS
	utility.dim_title
	(	public_name	STRING
	,	abbreviation	STRING
	,	game	STRING
	,	release	INTEGER
	,	game_base	STRING
	,	platform	STRING
	,	environment	STRING
	,	game_name	STRING
	,	release_date	DATE
	,	orig_release_date	DATE
	)
	STORED
		AS orc
	LOCATION
		's3://glu-emr/tables/utility.db/dimtitle'
;

-- migrate to orc
INSERT
	OVERWRITE
TABLE
	utility.dim_title
SELECT
	*
FROM
	utility.dimtitle_text
;

-- clean up old mess
DROP TABLE IF EXISTS
	utility.dimtitle_text
;

-- compute table stats for query optimizer
ANALYZE
TABLE
	utility.dim_title
COMPUTE
	STATISTICS
FOR	COLUMNS
;
