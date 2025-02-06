# Extreme_Weather_Conditions_and_Alerting
# Extreme Weather Conditions and Real-Time Alerting

In collaboration with Big Blue Data Academy, we developed a **real-time alerting system** for extreme weather conditions using data engineering techniques. Our project focuses on **analyzing historical weather patterns** and **predicting potential risks** to support agriculture and farming. The system provides real-time alerts for extreme weather events, helping farmers make informed decisions on irrigation, harvesting, and preventive measures.

## Duration:  
1 month  

## Project Organization  
├── README.md <- The top-level README for navigating this project 
├── Data <- Raw historical weather data for 25 cities in Greece 
├── ETL <- Scripts for extracting, transforming, and loading data 
├── API_Scripts <- Scripts for fetching real-time weather data from APIs 
├── Airflow_DAGs <- DAGs for orchestrating ETL processes and alerting system 
├── Dashboard <- Power BI dashboards for visualizing weather patterns 
├── Presentation <- Final project presentation and conclusions


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

- **Incremental Data Load:** Fetches only new records from APIs to minimize redundancy.
- **Full Data Load:** Processes large-scale historical data for pattern analysis.
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

- 📊 **Power BI Dashboards:** Interactive visualizations for weather trends and alerts.
- 📡 **Metabase Diagrams:** Data-driven insights for better decision-making.

## 🎯 **Final Results**  
___
Our system improves **real-time monitoring of weather conditions**, providing **valuable insights** for farmers and stakeholders. The **scalable architecture** allows easy integration with additional **data sources and predictive models**.

| Feature                  | Status          |
|--------------------------|----------------|
| ✅ Real-time weather alerts | Implemented   |
| ✅ Historical data analysis | Implemented   |
| 🚀 CI/CD for automation   | Future Work    |
| 📩 Email-based alerts    | Future Work    |
| 🌍 More cities/rural areas | Future Work    |

## 🔮 **Future Work**  
___
- **Incorporate more weather data sources** for better forecasting.
- **Implement CI/CD** for automated deployments.
- **Expand the system** to cover more **cities and rural areas**.
- **Enhance fault tolerance** and **error handling**.

## 👥 **Participants**  
___
- **Katerina Psallida**  
- **Dimitris Kasseropoulos**  

---

🚀 *This project is part of the Big Blue Data Academy, Data Engineering Bootcamp, February 2025.*




