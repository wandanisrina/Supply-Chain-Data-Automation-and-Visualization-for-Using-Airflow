# Analysis of Pharmaceutical Supply Chain Dataset : HIV & Malaria Medicines and Diagnostic Tools Shipment of SVT Distributor

## Repository Outline

```
1. README.md - Explanation of the project overview
2. DAG_Script.py - Pyhton file containing code for extracting, transforming until loading data to Elasticsearch
3. elastic-kibana.yaml - File containing script to run ETL process in docker
4. Data_Validation_withGX.ipynb - Notebook that contains data validation jusing Great Expectation
5. DDL_Syntax_to_PostgreSQL.txt - Text that contains link of dataset and code for inserting data to PostgreSQL
6. data_raw.csv - Raw dataset after inserted to PostgreSQL and before being transformed
7. data_clean.csv - Cleaned dataset after being transformed using Airflow
8. DAG_Graph_Flow.jpg - Screeshoot of DAG flow in Airflow
9. elastic-kibana.yaml - File containing script to run ETL process in docker
10. images - Contains visualization screenshoots from Kibana
```

## Problem Background

The supply chain system in the health sector plays a very crucial role in ensuring the availability of medical products. Failure in supply chain management can have a direct impact on shipment delays, increased logistics costs, and disruption of healthcare services. Dataset used is "Supply Chain Management System (SCMS) Delivery History" provides a comprehensive overview of drug product delivery activities to several countries, which contains data related to drug types, shipping processes, and logistics costs. In this dataset, it will focus on showing distributions of HIV & Malaria medicines and diagnostic tools to several countries that have prevalence of this disease.

This dataset is useful for viewing trends in pharmaceutical product distribution to several countries, so that it can be analyzed in the shipping process and prices. As a data analyst team from SVT Distributor, a pharmaceutical distributor that has branches in various countries, this analysis is useful for improving shipping services and determining more competitive prices. Moreover, it can be analyzed about the shipping process, product prices, and shipping costs of a pharmaceutical product. By analyzing this dataset, the target audiences, including top-level management, Supply Chain (Procurement & Logistics) team, and Marketing team are expected to gain insight into improving business efficiency.



## Project Output

The output of this project is data analytics that automates the processing of pharmaceutical supply chain data. Starting with inserting data using Python and PostgreSQL then passed through an Apache Airflow DAG with ETL (Extract, Transform, Load) process. In the airflow, the data will be recorded by schedule that already determined. The data is successfully loaded into Elasticsearch, serving as the backend for efficient querying and indexing. Finally, the indexed data is visualized in Kibana through interactive dashboards that display key insights such as sales trends, top vendors, freight cost proportions, and destination distributions. This output enables stakeholders to monitor and analyze the supply chain performance and supports data-driven decision-making across departments.


## Data

The dataset provides a comprehensive collection of Pharmaceutical Supply Chain from Kaggle. In this dataset, it contains 33 columns and 10.324 rows. There are missing values in some columns and no duplicates data rows.


## Method

In this analysis we divide 5 steps, which is ETL process, Airflow DAG,  Data Validation, Loading data to Elasticsearch, and visualization using Kibana. 
1. ETL Process: Cleaned and transformed raw dataset into a structured format without missing values.
2. Airflow DAG: Automated the scheduling and orchestration of the ETL workflow.
3. Data Validation : Validate cleaned dataset using GreatExpectation with output is suceess true
4. Loading data to Elasticsearch: Indexed the processed data for efficient querying and searchability.
5. Visualization using Kibana: Built interactive dashboards to gain insights from the indexed data.


## Stacks

Tools used for this analysis are VSCode, Pyhton, PostgreSQL, Docker, Airflow, and Kibana for visualization. 
Additional library that being used are pandas, numpy, GreatExpectation for data validation, DAG for data scheduling, and ElasticSearch for data indexing.


## Reference

- [Raw dataset link](https://www.kaggle.com/datasets/yashdalsaniya/supply-chain-delivery-dataset )
- [Project Repository](https://github.com/wandanisrina/Supply-Chain-Data-Automation-and-Visualization-for-Using-Airflow.git) (**Must read**)
