-- 
-- DESC	Create date dimension table
-- AUTH	Justin the Smith
-- DATE	2019-12-10
-- DEST	s3://glu-emr/tables/utility.db/dim_date
-- NOTE	Reduced from 10k to 2.5k rows because Hive's shitty query optimizer can't push predicates down over cross-joins.
--		I'd put a frownie-face emoji on this note but Hive's shitty parser sometimes fails on parens in comments!
--

-- configure program
set max_n=2500;

-- define temporary stuff
CREATE TEMPORARY FUNCTION numRange AS 'com.glu.hive.udf.NumRange';

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
set hive.strict.checks.cartesian.product=false;
set mapreduce.input.fileinputformat.split.maxsize=5000000;

-- create storage for output
DROP TABLE IF EXISTS
	utility.dim_date
;

CREATE EXTERNAL TABLE IF NOT EXISTS
	utility.dim_date
	(	n	BIGINT
	,	the_date	DATE
	,	week_num_of_year	INTEGER
	,	day_num_of_week	INTEGER
	,	day_of_week	STRING
	,	is_weekend	INTEGER
	,	dateid	BIGINT
	,	yearid	INTEGER
	,	monthid	INTEGER
	,	dayid	INTEGER
--	)
--	PARTITIONED BY
	,	yy	string
	,	mm	string
	,	dd	string
	)
	STORED AS orc
	LOCATION 's3://glu-emr/tables/utility.db/dim_date'
;

WITH
config	AS
(	SELECT
		DATE('2015-01-01')	AS start_date
--	,	10000	AS maxn							-- SQL
)
-- SQL:
-- need some data (SQRT(maxn) rows) to create some fake rows
--,base	AS (
--	SELECT
--		1	AS test
--	FROM
--		device_metrics.game_install
--	WHERE	-- filter to limit cross join
--		year	= '2019'
--	AND	month	= '01'
--	AND	day	= '01'
--	AND	game_name	= 'TapSportsBaseball18_ANDROID_PROD'
--	AND	country	= 'Canada'
--)
--,numbers	AS (
--	SELECT
--		ROW_NUMBER() OVER()	AS n
--	FROM
--		base	AS b1
--	,	base	AS b2
--)
--,dates	AS (
--	SELECT
--		n
--	,	DATE_ADD('day', n - 1, start_date)	AS the_date
--	FROM
--		numbers
--	,	config
--	WHERE
--		n	<=	maxn
--)
-- HIVE:
,
numbers	AS
(	SELECT
		t.n
	,	start_date
	FROM
		config
	LATERAL VIEW
		explode(numRange(0, ${hiveconf:max_n}, 1)) t	AS n
	)
,
dates	AS
(	SELECT
		n
	,	DATE_ADD(start_date, n)	AS the_date
	FROM
		numbers
)
,
-- all together now!
datetable	AS
(	SELECT
		*
--	,	WEEK(the_date)	AS week_num_of_year								-- SQL
--	,	DOW(the_date)	AS day_num_of_week								-- SQL
	,	WEEKOFYEAR(the_date)	AS week_num_of_year						-- Hive
	,	CAST(DATE_FORMAT(the_date, 'u') AS INTEGER)	AS day_num_of_week	-- Hive
	,	YEAR(the_date)	AS yearid
	,	MONTH(the_date)	AS monthid
	,	DAY(the_date)	AS dayid
--	,	CAST(YEAR(the_date) AS VARCHAR)	AS yy							-- SQL
--	,	LPAD(CAST(MONTH(the_date) AS VARCHAR), 2, '0')	AS mm			-- SQL
--	,	LPAD(CAST(DAY(the_date) AS VARCHAR), 2, '0')	AS dd			-- SQL
	,	CAST(YEAR(the_date) AS STRING)	AS yy							-- Hive
	,	LPAD(CAST(MONTH(the_date) AS STRING), 2, '0')	AS mm			-- Hive
	,	LPAD(CAST(DAY(the_date) AS STRING), 2, '0')	AS dd				-- Hive
	FROM
		dates
)
INSERT OVERWRITE TABLE
	utility.dim_date
--PARTITION
--(	yy
--,	mm
--,	dd
--)
SELECT
	n
,	the_date
,	week_num_of_year
,	day_num_of_week
,	CASE
	WHEN	day_num_of_week	= 1	THEN	'Monday'
	WHEN	day_num_of_week	= 2	THEN	'Tuesday'
	WHEN	day_num_of_week	= 3	THEN	'Wednesday'
	WHEN	day_num_of_week	= 4	THEN	'Thursday'
	WHEN	day_num_of_week	= 5	THEN	'Friday'
	WHEN	day_num_of_week	= 6	THEN	'Saturday'
	ELSE	'Sunday'
	END	AS day_of_week
,	CASE
	WHEN	day_num_of_week	IN (6, 7)
	THEN	1
	ELSE	0
	END	AS is_weekend
,	CAST(CONCAT(yy, mm, dd) AS BIGINT) 	AS dateid
,	yearid
,	monthid
,	dayid
,	yy
,	mm
,	dd
FROM
	datetable
ORDER BY
	n
;

-- compute table stats for query optimizer
ANALYZE
TABLE
	utility.dim_date
--PARTITION
--(	yy
--,	mm
--,	dd
--)
COMPUTE
	STATISTICS
FOR	COLUMNS
;
