# Insight Project: E-Monitor
# Project Idea

My project is a real-time ECG monitoring system for health-care providers to monitor their patient's live ECG results. 

# Practical Significance

In the U.S. alone, yearly spending on healthcare can be as high as 980 billion. One costly part of healthcare services is the monitoring of the patient’s vital signs and physiological signals. Medical facilities has their own monitoring equipment that can treat patients who go to these facilities. However, in-home care is believed to be one of the most effective ways for addressing increasingly severe chronic diseases. My project is a POC of a real-time ECG streaming system for hospitals to monitor at-home patients and their vital signs. The aim of this project is to process massive amount of real-time, critical data with low latency and present results to the dashboard.

# Data pipeline
## Data Extraction
The raw patient history data comes from the biggest public healthcare dataset in the U.S. It can be found here: https://physionet.org/physiobank/database/cves/. The historical patient data in about 100GB in size. To get the streaming data, I used a processed CSV data set from Kaggle, which can be found here: https://www.kaggle.com/shayanfazeli/heartbeat. The extraction step includes ingesting patient data directly into the db and putting to-be streamed data into S3. The streaming data is cleaned and combined together in 1 file so the size is large enough to stream for 30 seconds.

## Data Transformation 
The main transformation for this project is to analyze the incoming data and filter based on heartbeat classification. The normal heartbeat is directly stored and the problematic streaming data is joined with the patient history data from the database. The problematic data is sent to the WebUI. There are a total of 300,000 records coming in and the patient historical ECG data contains about 5M records. 

## Data Storage and Loading
The data is stored in a Cassandra Time-Series database in 3 tables. One table is the patient information, one table is the raw data and the other is the displayed results. The WebUI listens on the results table and list the problematic ECG data.

# Challanges
## Data Ingestion
The first challange faced is for Kafka producers to send data out 

S3 to store the simulated data\
Kafka and Spark Streaming to load and process data \
Cassedra to store the results for further analysis and alert Paramedics \
Flask to present result in a dashboard
# Data Source

100 GB in size.

# Engineering Challange
1. low lantency when alerting
2. Map the result to the appropriete paramedic's location
# MVP
1. A working pipeline and dashboard with 1GB of data
