## Introduction

This repository contains the final data warehouse constructed by the Group - Lakers Showtime - of 3CSC from the University of Santo Tomas as a fulfillment of the team's Data Warehouse course.

Moreover, this repository consists of a complete, end-to-end data pipeline, including all required Python scripts, SQL queries, Docker configuration files, raw and processed data files, and an interactive Power BI dashboard file. Each component is designed to work in harmony, collectively supporting ShopZada’s business needs through an effective ETL (Extract, Transform, Load) process that will be described further in the section below.

## Architecture Overview

The team's created a Kimball Style data warehouse for Shopzada since a Kimball architecture is simpler and faster to set up, provides ShopZada with the needed analytics based on its large amount of data, is easy to scale for future their future needs, and is intended to make it convenient for ShopZada employees to view, query, understand, and use their data in an organized manner. 

Moreover, the Kimball Style Data Warehouse consists of 6 Dimensions (dim_Campaign, dim_Date, dim_Merchant, dim_Product, dim_Staff, and dim_Users) and 3 Fact tables (Fact_Line_Item, Fact_Campaigns, and Fact_Orders). Each of these components combine to create an efficient and scalable data architecture again fulfilling the business needs of ShopZada.

## ETL Process Overview

The ETL Process consists of 5 stages. First is the extraction of raw data. Second, set up the Directories (e.g., file structure) and clean the data. Next is the staging stage, where the data is transferred to a staging layer before being loaded directly into the dimensions. Afterwards, loading is performed. Finally, the data loaded into PostgreSQL will be queried to be displayed in the Power BI analytical dashboard.

Link to detailed documentation: https://docs.google.com/document/d/1MbajIyuU7LlBti3x8vlTPUzMr1JoFpkFX39NvAysZqU/edit?tab=t.0


## How to Run the Project

  

### Prerequisites

Make sure the following are installed on your system:

- Docker Desktop
- Git
- Any web browser to access Airflow and PostgreSQL.
- Power BI


---

  

### 1. Clone the Repository

```bash
git  clone https://github.com/n4t5Uuu/dwh_finalproject_3CSC_group_Lakers-Showtime.git
cd <project-root>
```

---

### 2. Start the Environment

```bash
cd  infra
docker compose up -d
```
---
  

### 3. Access Airflow

Open a browser and navigate to:
```bash
http:localhost:8999 # Airflow

# Login Credentials:
username = admin
password = admin
```
Once logged in, un-pause all DAGs and run **"dag_master_shopzada_pipeline.py"**. This whole dag might take a while to finish (around 7 minutes). 

**BE AWARE THAT THE SCRIPTS WILL CREATE MANY ".csv" FILES UNDER THE "clean_data" FOLDER AND WILL  CONSUME AROUND 500MB OF STORAGE**

### (Optional) Access PostgreSQL 
You can access PostgreSQL in order to check the staging, dimensions, and fact tables.
```bash
http:localhost:5050 # PostgreSQL

# Login Credentials
username = admin@admin.com
password = admin
```
After logging in, right click **"Servers"** under "Object Explorer" in the left-hand panel. Then, left click **"Register a Server"**. A window will appear and you can input the following: 
```bash
General Tab:
- Name 					= shopzada

Connection Tab:
- Hostname 				= postgres
- Port 					= 5432
- Maintenance database 	= airflow
- Username 				= airflow
- Password 				= airflow
```
Once done, you can check the dimensions and fact tables under the "shopzada" schema. The staging tables are located under the "staging" schema.

---

### 4. View Dashboard via Power BI

Navigate to the **"dashboard"** folder. 

Right-click the **"analytics_dashboard.pbix"** file and press **"Reveal in File Explorer"**
This will open the file explorer, where you can **open the "analytics_dashboard.pbix" file using Power BI**. 

Opening the analytics dashboard displays data organized into tabs: Executive Dashboard, Campaign, Orders, Merchant and Staff, and Users.


---
