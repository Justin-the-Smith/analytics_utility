# analytics_utility
Utility dimension tables for analytics

To update:
1.	Download *.csv file in text/ folder
2.	Edit *.csv file
>$	vi dimtitle.csv
3.	Upload *.csv file to S3 utility folder
>$	aws s3 cp dimtitle.csv s3://glu-emr/utility/dimtitle/
4.	Run Hive query on EMR
>$	hive -f dimtitle.hql
