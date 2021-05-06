## Data Engineering 


### A Hands-On, Project-Based instruction for Data Engineering ETL

This is a collection of resources for data engineering ETL for a fictious a music company.

Email: gupt.rakeshk@gmail.com

This project walks through end-to-end data engineering steps that are needed in a typical project.
Model user activity data to create a database and ETL pipeline in Postgres for a music streaming app. 
Define Fact and Dimension tables and insert data into new tables.

Steps involved are :
- Creating data models that is needed to cature structured data. Here it is using PostgreSQL as RDBMS.
- Read data resources that are collected from various channels 
- Apply ETL (extract, transform and load) to create pipeline using PostgreSQL.
- Once data is cleansed, transformed and load, it is ready to asnwer queries for analytics.


Here are helpful steps in executing python programs in right sequence. You must execute **create_table.py** first 
in order to create database tables which are needed for storing fact and dimension data.
1. execute `python create_table.py` from CLI or other interface 
2. execute `python etl.py` from CLI or other interface 

Additional tips
- One can use `etl.ipynb` Jupyter notebook to try code snippet for validation of logic.
- One can use `test.ipynb` Jupyter notebook to validate data is successfully inserted into tables after ETL.


