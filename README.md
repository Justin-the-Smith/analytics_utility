# analytics_utility
Utility dimension tables for analytics

To update:
1.	Download source data to EMR: text/&ast;.csv
2.	Download Hive query to EMR: hql/&ast;.hql
3.	Edit &ast;.csv data file
>$	vi dimtitle.csv
4.	Upload &ast;.csv data file to S3 utility folder
>$	aws s3 cp dimtitle.csv s3://glu-emr/utility/dimtitle/
5.	Run Hive query on EMR
>$	hive -f dimtitle.hql
