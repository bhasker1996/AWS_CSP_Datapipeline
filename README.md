# AWS CSP Data Pipeline
AWS Data Flow to put data from S3 or API Gateway to the redshift using lambda

# AWS Serverless Data Pipeline — S3 → Lambda → Redshift (Glue, Athena)→ QuickSight

This project demonstrates a simple, fully serverless data pipeline built using AWS services.  
The pipeline ingests raw CSV/JSON files from an S3 bucket, transforms them using AWS Lambda,  
and loads the processed records into an Amazon Redshift table.  
A QuickSight dashboard is connected to Redshift for visualization.

This project was built to practice and demonstrate:
- ETL pipeline design (without Glue)
- Writing Lambda functions in Python
- Redshift data loading
- S3 event-driven architecture
- Creating dashboards in QuickSight

---

## 📁 Repository Structure
```
├── architecture/
│ └── architecture.png
├── lambda/
│ └── lambda_function.py
├── redshift/
│ ├── table_schema.sql
│ └── sample_query.sql
├── quicksight/
│ └── dashboard.png
└── README.md
```

---

## 🏗️ Architecture

<p align="center">
  <img src="architecture/CSP Tools Architecture - Phase1.png" width="700"/>
</p>

**Flow Explanation:**
1. File is uploaded to **Amazon S3**
2. **S3 Event Trigger** invokes a **Lambda function**
3. Lambda reads the file → transforms the data → inserts into **Amazon Redshift**
4. **QuickSight Dashboard** reads the data directly from Redshift

---

## 🔧 Lambda Function (ETL Logic)

The Lambda function performs:
- Reading CSV/JSON files from S3  
- Basic cleaning and transformation  
- Connecting to Redshift using `psycopg2`  
- Inserting records into the table row-by-row  
- Avoiding duplicate loads using a simple date check  

The full code is located here:  
👉 `lambda/lambda_function.py`

---

## 🗄️ Redshift Table Schema

This project uses **a single Redshift table**:

CREATE TABLE tool_usage_metrics (
id INTEGER IDENTITY(1,1),
team_name VARCHAR(100),
tool_name VARCHAR(100),
usage_count INTEGER,
report_date DATE
);


SQL queries used in QuickSight analysis can be found in:  
👉 `redshift/sample_query.sql`

---

## 📊 QuickSight Dashboard

<p align="center">
  <img src="quicksight/Quicksight Dashboard.png" width="700"/>
</p>

The dashboard includes:
- Tool usage by team
- KPI metrics
- Bar chart to show which tools are to be active and inactive
- Filters for team, tool, and date  

---

## 🧪 Example Input File (Uploaded to S3)
team_name,tool_name,usage_count,report_date
Analytics,QuickSight,55,2024-11-01
Marketing,Sheets,33,2024-11-01
Support,ServiceNow,41,2024-11-01


---

## 🚀 Pipeline Execution — Step by Step

1. Upload `team_data.csv` to S3  
2. Lambda is automatically triggered  
3. Lambda processes and inserts data into Redshift  
4. QuickSight refreshes and displays updated metrics  

---

## 📝 Future Enhancements

- Add Glue for heavy ETL workloads  
- Add more tables for dimensional modelling  
- Implement automated scheduling using EventBridge  
- Deploy pipeline with AWS CDK  

---

## 👨‍💻 Author  
**Bhasker Saiteja**  
AWS • Python • Data Engineering • Cloud Automation
