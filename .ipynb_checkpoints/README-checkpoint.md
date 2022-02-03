# Introduction of this project:

Sparkify is a music streaming startup. Since the business of Sparkify grows fast, they decided to move their user and song data onto cloud for more flexibile and scalable database services. 
Right now, their data are in AWS S3. The data engineers in Sparkify want to build a ETL pipeline to extract their data from S3, stage them in Redshift, and transferm data into a set of dimensional tables for the analytics team to conduct deep analysis efficiently. 



# Database schema design and ETL pipeline

I choose Star schema for Sparkify. Because star schema can reduce joins and increase efficiency for analysis.

songpalys table is the fact table. songplay_id is the primary key. Distribution style for songplays table is key because the table is too large to distribute by all.

users table is a dimensional table with user_id as primary key. It is not a big table so that the distribution style can be all.

songs table is a dimentional table with song_id as primary key. The distribution style for song table is all.

artists table is a dimentional table with artist_id as primary key. The distribution style for artists table is all.

time table is another dimentioanl table with start_time as primary key. The distribution style for artists table is all.


# Project steps:

- Design schemas for fact and dimension tables. 
- Use create_table.py to create tables. 
- Launch redshift cluster and create an IAM role to read access to S3.
- Build ETL pipeline to load data from S3 to staging tables on Redshift.  
- Load data from staging table to analytics tables on Redshift.
- Test pipeline.







