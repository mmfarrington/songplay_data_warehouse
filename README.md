# Cloud Data Warehouse Implementation in AWS
## Udacity Data Engineering Nanodegree
### Submitted by: Miriam Farrington 
----
## Introduction
Nanodegree Program Overview 
(see [Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027))

> In this project, I was tasked with building an ETL pipeline that extracts song records files from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights in what songs their users are listening to".

----
## Instructions

1. Instantiate a new cluster in Redshift ensuring proper IAM role and VPC groups are attached. 
2. Copy cluster endpoint, IAM role ARN and db credentials into dwh.cfg
3. Open console and import python files: 
    
         import create_tables as ct
         import etl

4. Run main() method in each file sequentially to perform table setup and etl operations: 

         ct.main()
         etl.main()

----
## Data Visualization

See Level Analysis Dashboard tracks songplay metrics based on User Tier (free vs. paid)

----
## Changelog
* 7-Jul-2020 initial commit