# analytics_utility
Utility dimension tables for analytics

To update:
1.	Download *.csv file in text/ folder
2.	Edit *.csv file
	e.g.:$	vi dimtitle.csv
3.	Upload *.csv file to S3 utility folder
	e.g.:$	aws s3 cp dimtitle.csv s3://glu-emr/utility/dimtitle/
4.	Run Hive query
	e.g.:$	hive -f dimtitle.hql
