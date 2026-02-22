# **Noora Health Data Engineer Assignment \- README**

## **1\. Prerequisites**

* Python 3.9+	  
* Jupyter Notebook  
* Google Cloud project access  
* BigQuery enabled  
* Airbyte Cloud access  
* Service Account JSON key (created below)

# **2\. Google Cloud Setup (BigQuery)**

## **2.1 Create Datasets**

In BigQuery, create two datasets:

* noora\_health\_raw  
* noora\_health\_analytics

## **2.2 Create Service Account**

* Go to: IAM & Admin → Service Accounts  
* Click Create Service Account  
* Name:noora-health-loader  
* Assign roles:  
  BigQuery Data Editor  
  BigQuery Job User  
* Click Done

## **2.3 Generate JSON Key**

1. Open the service account.  
2. Go to Keys.  
3. Click Add Key → Create New Key → JSON.  
4. Download the file.

# **3\. Airbyte Setup**

## **3.1 Host CSV Files**

Upload:

* messages.csv  
* statuses.csv

to a public GitHub repository.  
Use RAW URLs in this format: https://raw.githubusercontent.com/\<username\>/\<repo\>/main/messages.csv

Do NOT use github.com/.../blob/...

## **3.2 Create BigQuery Destination**

In Airbyte:

1. Create new Destination → BigQuery  
2. Paste entire Service Account JSON content  
3. Set Dataset:  
    noora\_health\_raw  
4. Test connection

## **3.3 Create Source – Messages**

1. Create Source → File → HTTPS Public Web  
2. Paste RAW GitHub URL for messages.csv  
3. Format \= CSV  
4. Encoding \= UTF-8  
5. Sync Mode \= Full Refresh | Overwrite  
6. Run Sync

Confirm table created: noora\_raw\_files

## 

## **3.4 Create Source – Statuses**

Repeat process for statuses.csv.  
Confirm table created: noora\_statuses\_file

# **4\. Data Transformation**

Run:transform\_messages.sql (The query is available in the Noora Health Data Engineer Assignment \- Code Documentation.pdf file)

This script:

* Deduplicates append-only messages using `ROW_NUMBER()`  
* Aggregates full status lifecycle using `ARRAY_AGG`  
* Parses timestamp fields using `PARSE_TIMESTAMP`  
* Includes user identifiers  
* Creates: noora\_health\_analytics.messages\_enriched  
* Verify row count: 14,746

# **5\. Data Validation**

Run: validations.sql  (The query is available in the Noora Health Data Engineer Assignment \- Code Documentation.pdf file)

This validates:

* Duplicate messages (identical content within 60 seconds)  
* Referential integrity (no orphan statuses)  
* Lifecycle integrity (read never before sent)  
* Outbound completeness

# **6\. Visualization Setup (Jupyter)**

## **6.1 Install Dependencies**

pip install google-cloud-bigquery pandas plotly db-dtypes

## **6.2 Authenticate**

**Option A** – Service Account:

import os

os.environ\["GOOGLE\_APPLICATION\_CREDENTIALS"\] \= "path/to/service\_account.json"

**Option B** – gcloud (run in terminal):

gcloud auth application-default login

Do not run gcloud inside Jupyter.

## **6.3 Run Notebook**

Open:noora\_health\_assignment.ipynb

Copy and Run all cells.

The notebook generates:

* Total vs Active Users (weekly)  
* Read rate (non-failed outbound)  
* Time-to-read distribution  
* Outbound status breakdown (last 7 days anchored to dataset max date)

# **7\. Important Notes**

### **Timestamp Handling**

Airbyte loads timestamps as STRING.

All timestamps must be parsed using:

PARSE\_TIMESTAMP('%m/%d/%Y %H:%M:%S', column)

### **Last 7 Days Logic**

Do NOT use `CURRENT_DATE()`.

Use: MAX(status\_date)

to anchor the rolling window because the dataset is historical.

### **User Definition**

Define user\_id as:

* `masked_author` for inbound messages

* `masked_addressees` for outbound messages

# **8\. Expected Final State**

After full execution:

* Raw tables in noora\_health\_raw  
* Analytical table in noora\_health\_analytics  
* 14,746 rows in messages\_enriched  
* Validation queries return expected results  
* Visualizations render successfully

# **9\. Pipeline Summary**

This project implements:

* Raw ingestion layer (Airbyte → BigQuery)  
* Analytical modeling layer  
* Event lifecycle preservation  
* Data validation framework  
* Reproducible visualization workflow

