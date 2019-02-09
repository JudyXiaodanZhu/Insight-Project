# Insight-Project
# Project Idea

My system is a real-time monitoring system of the patient's ECG data and alerting system for the hospital.

# Business Value 

In the U.S. alone, yearly spending on healthcare can be as high as 980 billion. One costly part of healthcare services is the monitoring of the patient’s vital signs and physiological signals. Medical facilities has their own monitoring equipment that can treat patients who go to these facilities. However, in-home care is believed to be one of the most effective ways for addressing increasingly severe chronic diseases. My project is a POC of a real-time ECG streaming system for hospitals to monitor at-home patients and their vital signs. The aim of this project is to processes massive amount of real-time, critical data with low latency.  

# Teck Stack
S3 to store the simulated data\
Kafka and Spark Streaming to load and process data \
Cassedra to store the results for further analysis and alert Paramedics \
Flask to present result in a dashboard
# Data Source
https://physionet.org/physiobank/database/cves/
100 GB in size.

# Engineering Challange
1. low lantency when alerting
2. Map the result to the appropriete paramedic's location
# MVP
1. A working pipeline and dashboard with 1GB of data
