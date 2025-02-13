# Extreme Weather Conditions and Real-Time Alerting

As a final project for graduating from the Data Engineering Bootcamp organized by the Big Blue Data Academy, we developed a **real-time 
alerting system** for extreme weather conditions using data engineering techniques. 
#
Our project focuses on acquiring the necessary data from APIs, in an organized and automated manner, in order to clean, save into a database and provide to data analysts / data scientists for further analysis.

## Duration:  
2.5 weeks   

## Project Organization  

📄 README.md  
  - The top-level README for navigating this project  

📂 notebooks/  
  - Scripts for extracting, transforming, and loading data  
  📂 AirflowContainers/  
    - Scripts for initiating Airflow in Containers  
  📂 AirflowDags/  
    - Scripts for creating DAGs for orchestrating ETL processes and alerting system  

📂 dashboard/  
  - Power BI dashboards for visualizing weather patterns  

📂 presentation/  
  - Final project presentation and conclusions  


## 📊 **Data**  
___
The project utilizes **1.2 million rows** of **hourly historical weather data** spanning from **01-01-2020 to 31-01-2025**, along with **10K forecast records** for extreme weather conditions.

### **Data Sources:**
- 🌍 **Open-Meteo API** – Provides historical weather data and 16-day forecasts.
- 🏙️ **Nominatim API** – Used for retrieving city coordinates.
- ☁️ **National Centers for Environmental Information** – Standardized weather codes.

## ⚙️ **ETL Process**  
___
To ensure data accuracy and consistency, we implemented **ETL pipelines** using **Apache Airflow**:

- **Full Data Load:** Processes large-scale historical data for pattern analysis.
- **Incremental Data Load:** Fetches only new records from APIs to minimize redundancy.
- **Data Enrichment:** Enhances raw data with computed weather indicators.
- **Airflow DAGs:** Automate and orchestrate data pipelines for real-time updates.

## 🚨 **Real-Time Alerting System**  
___
Our real-time alerting system is powered by **Apache Airflow** and messaging services:

- ⚠️ **Alerts for extreme weather events** (e.g., frost, heatwaves, heavy rainfall).
- 📩 **Automated notifications via Discord messages** for real-time updates.
- 📧 **Future work includes expanding to email-based alerts**.

## 📈 **Visualization & Insights**  
___
We use powerful dashboards to visualize trends and alerts:

- 📊 **Power BI Dashboards:** Interactive visualizations for weather trends and historical conditions.
- 📡 **Metabase Diagrams:** Data-driven insights for better decision-making.


## 👥 **Participants**  
___
- **Katerina Psallida**  
- **Dimitris Kasseropoulos**  

---

## 🌟 Project Highlight  
___  

![Project Snapshot](presentation/Highlight.png)  

The Docker daemon listens to docker.sock, allowing it to oversee all Docker containers and their communication with each other.

Thus, on the same machine, where the Airflow container and the Metabase container are deployed,  we also launch a temporary container via the Airflow Docker Operator, which provides the necessary environment for Airflow to run the 'Incremental Upload' Python Script. This utilizes an image that has to be known to the machine we are running the container or be present in an Image repository and pulled from there. Also it utilizes a shared mount, so that Airflow container and the temporary container share the same required files.

This is a `highlight` since we do not need to install Python on our VM or any other software. Instead, we create a temporary environment, with shared files in which we may also allocate the resources needed (i.e memory, cpu).


🚀 *This project is part of the Big Blue Data Academy, Data Engineering Bootcamp, February 2025.*




