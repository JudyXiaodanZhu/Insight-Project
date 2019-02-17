# E-Monitor
The goal of my Insight Data Engineering project was to design a data pipeline and Web UI for monitoring ECG records and display problematic patient information. My data could be used by healthcare providers to be alerted about life-threathing events or to diagnose Cardiac Arrhythmia in real-time.

## Background

In the U.S. alone, yearly spending on healthcare can be as high as 980 billion. One costly part of healthcare services is the monitoring  of the patient’s vital signs and physiological signals. Medical facilities has their own monitoring equipment that can treat patients who go to these facilities. However, in-home care is believed to be one of the most effective ways for addressing increasingly severe chronic diseases. 

Cardiac Arrhythmia (heart irregularity) is a severe chronic heart disease that affects roughly 10 millions (2-3%) of the population in the U.S. There can be various causes to heart arrhythmia and and it can lead to life threathening problems such as heart failure or strokes. One traditional way to diagnose Cardiac Arrhythmia is to provide patients with a Holter monitor, which is a machine with sensors that collects patient information for 24h or 48h. After 1 day, the machine is taken to healthcare providers and data is extracted from the machine to perform analysis. Nowadays, with the emergence of mobile phones and real-time analytics, patients can wear wireless sensors and have their phones pass the sensor information to healthcare providers in real-time. The new process can significantly shorten the diagnosis time and also allow real-time monitoring for any critical issues.  


## Functional Requirements
After analysing the above real-life use cases, the main goals for my project has been determined. This project should be able to process massive amount of real-time data, present the results to the dashboard for critical-issue monitoring and provide an efficient way to query recent history for doctors to perform analytics. The functional requirements are listed below:

1. An infrastructure that can ingest, process, store and query massive amount of real-time/historical ECG data.
2. A dashboard that displays problematic ECG results and related patient information.
3. Although the project timeline is only three weeks and the amount of data processed will be limited, at the initial stages of the the design process, this project should still be regarded as a large-scale project and scalability consideration is a must.


## Data pipeline
### Data Extraction
The raw patient history data comes from the biggest public healthcare dataset in the U.S. It can be found here: https://physionet.org/physiobank/database/cves/. The historical patient data in about 100GB in size. \

To get the streaming data, I used a processed CSV data set from Kaggle, which can be found here: https://www.kaggle.com/shayanfazeli/heartbeat. The extraction step includes ingesting patient data directly into the db and putting to-be streamed data into S3. The streaming data is cleaned and combined together in 1 file so the size is large enough to stream for 30 seconds.

### Data Transformation 
The main transformation for this project is to analyze the incoming data and filter based on heartbeat classification. The normal heartbeat is directly stored and the problematic streaming data is joined with the patient history data from the database. The problematic data is sent to the WebUI. There are a total of 300,000 records coming in and the patient historical ECG data contains about 5M records. 

### Data Storage and Loading
The data is stored in a Cassandra Time-Series database in 3 tables. One table is the patient information, one table is the raw data and the other is the displayed results. The WebUI listens on the results table and list the problematic ECG data.

## Challanges
### Data Ingestion
The first challange faced is to send 300,000 records of data in real-time into the system. At first, using one producer and one consumer, only 100 records can be sent per second. After adding more user threads, changing batch size, compression type and linger.ms for the producers, Kafka can process 10,000/records/s.

### Db Schema Design


## Tech stack choices
After carefulling considering the functional requirements and constrains, I designed the following data pipeline for my project:
1. S3 to store the simulated data
2. Kafka and Spark Streaming to load and process data 
3. Cassandra to store the raw and processed data 
4. Flask to present result in a dashboard
