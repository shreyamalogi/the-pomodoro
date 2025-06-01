
# 🛒 RetailStore Big Data Analysis with PySpark on GCP

## 🚀 Project Summary

This project demonstrates how to perform large-scale retail data analytics using **PySpark** on **Google Cloud Platform (GCP)** with a dataset of over **5 million records**. It covers multiple data processing workflows, using **Dataproc Clusters**, **GCS Buckets**, and **Spark Jobs** to produce actionable business insights.

From calculating average salaries by country to understanding customer distribution and purchasing power, this project showcases scalable and efficient data processing in a real-world retail scenario.

---

## 📌 Key Features

* ⚡ Processed **5M+ retail records** from GCS using PySpark
* 🔍 Performed **multi-dimensional analysis**: salary, age, gender, geography
* 📊 Combined all analysis outputs into a **single CSV report** using Spark transformations
* ☁️ Deployed and executed on **Google Cloud Dataproc**, demonstrating real-world cloud computing skills
* 🔐 Handled large datasets and structured outputs into **GCS buckets**
* ✅ Covered **ethical considerations** in big data usage (e.g. privacy, consent, and fairness)

---

## 💡 Analyses Performed

The following business insights were generated from the dataset:

1. **Average Salary by Country**

   * Identified purchasing power by region.

2. **Gender Distribution**

   * Determined demographic balance across records.

3. **Average Age of Customers**

   * Estimated customer maturity and segmenting potential.

4. **Total Customers by Country**

   * Found top-performing markets.

5. **Total Salary Spend by Country**

   * Prioritized regions with highest economic engagement.

Each output was saved to a **separate folder** (Script 1) or **combined into one single CSV** (Script 2), both in a **GCS bucket**.

---

## 🧰 Technologies Used

| Technology            | Purpose                               |
| --------------------- | ------------------------------------- |
| **PySpark**           | Distributed data processing           |
| **GCP Cloud Storage** | Scalable data storage                 |
| **GCP Dataproc**      | Cluster orchestration for Spark jobs  |
| **Python**            | Scripting and transformations         |


---

## 🏗️ Architecture

```
GCS Bucket (CSV)
       ↓
Dataproc Cluster
       ↓
PySpark Jobs (via Scripts)
       ↓
Transformations & Aggregations
       ↓
Clean Output Files in GCS
```

---

## ⚖️ Ethical Considerations

Inspired by real-world scenarios like “Big Retail Corp”:

* **Privacy vs. Personalization**: Are customers truly informed of how much data is collected?
* **Consent**: Are Terms of Use accessible and understandable?
* **Vulnerability**: Should predictive targeting avoid financially stressed customers?
* **Transparency**: Should companies proactively explain how data is used?
* **Security**: What if a breach leaks sensitive information?

These issues were actively discussed and considered throughout the analysis pipeline.

---
## 🧭 Ethical Case Study: Big Retail Corp

DATASET: https://drive.google.com/file/d/1mXTHLRpan4kyy_z0nTE7_mSPxIDkjZX0/view

### Background

Big Retail Corp launched an app collecting real-time data (location, preferences, income, etc.). While it enables personalized offers, it raised **ethical concerns** around:

- Constant background tracking
- Targeting financially vulnerable users
- Opaque data usage and privacy terms

---

## 🧾 Ethical Q&A Summary

| ❓ Ethical Question                             | 💬 Short Answer                                                                 |
|------------------------------------------------|---------------------------------------------------------------------------------|
| Is personalization worth the privacy tradeoff? | Only if **transparent**, **optional**, and **non-intrusive**.                  |
| Is consent via unread T&Cs valid?              | Not really. Companies must use **simple, clear, readable** disclosures.        |
| Should vulnerable users be targeted?           | No — targeting should not exploit emotional or financial weaknesses.           |
| How to increase data transparency?             | Use **dashboards**, **summarized policies**, and real-time tracking opt-outs.  |
| Who’s responsible during a data breach?        | The company. Must have **strict security protocols** and **response plans**.   |

---


## 🛠️ How to Run

### Pre-Requisites

* GCP account with permissions for Dataproc & Storage
* Python environment with PySpark installed (`pip install pyspark`)
* CSV file (`retailstore_5mn.csv`) uploaded to GCS bucket

### Execution Steps

1. **Create Dataproc Cluster**

   * Via UI or:

     ```bash
     gcloud dataproc clusters create my-cluster --region=us-central1
     ```

2. **Upload Script & Dataset** to GCS

   * e.g., `gs://my-bucket/retailstore_5mn.csv`

3. **Submit the Job**

   * For **multiple CSV outputs** (script 1):

     ```bash
     gcloud dataproc jobs submit pyspark retail_analysis_split.py \
       --cluster=my-cluster --region=us-central1
     ```
   * For **combined single CSV output** (script 2):

     ```bash
     gcloud dataproc jobs submit pyspark retail_analysis_combined.py \
       --cluster=my-cluster --region=us-central1
     ```

4. **View Results**

   * Navigate to your GCS output folder (e.g., `gs://my-bucket/output/`) to download and analyze CSVs.

---

