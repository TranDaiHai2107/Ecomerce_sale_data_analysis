# Office Furniture Sale Analytics

## 📌 Project Overview
This project aims to analyze e-commerce transaction data to uncover business insights, optimize sales performance, and understand customer behavior. By integrating data processing with Python/SQL and interactive visualization in Power BI, this repository provides an end-to-end data analytics solution. 

**Key Business Goals:**
- Track overall sales, profitability, and shipping performance.
- Identify the root causes of order cancellations.
- Perform Customer Segmentation (RFM Analysis) to identify high-value customers.

## 🛠️ Tech Stack
- **Data Processing & Cleaning:** Python (Pandas, NumPy) / SQL
- **Data Visualization:** Power BI
- **Advanced Analytics:** RFM Analysis, Exploratory Data Analysis (EDA)

## 🗂️ Dataset Description
The dataset consists of a relational database with 5 interconnected tables:
- `customer.csv`: Customer demographics (Age, Gender).
- `product.csv`: Product catalog, SKU, types, and unit prices.
- `orders.csv`: Transaction records, including a complex `'quantity':'cost'` column that requires parsing, order status, and feedback.
- `payment.csv`: Payment methods (e.g., Credit Card, Paypal, Cash).
- `shipping.csv`: Shipping categories and cost levels.

## 🚀 Key Features & Pipeline
1. **Data ETL (Extract, Transform, Load):** - Parsed and extracted numerical values from the JSON-like string column (`'quantity':'cost'`) in the Orders table.
   - Handled missing values, formatted datetime columns, and merged datasets via a Star Schema approach.
2. **Exploratory Data Analysis (EDA):** - Analyzed cancellation rates across different payment methods and shipping types.
3. **Interactive Power BI Dashboard:** - Built a dynamic dashboard to monitor KPIs (Total Revenue, Profit Margin, Cancellation Rate).
4. **Customer Segmentation:** - Applied RFM (Recency, Frequency, Monetary) metrics to segment the customer base for targeted marketing.

## 📁 Repository Structure
```text
├── data/
│   ├── raw/                 # Original CSV files
│   └── processed/           # Cleaned data ready for BI tools
├── notebooks/               
│   └── EDA_and_RFM_Analysis.ipynb  # Python data cleaning & analysis
├── dashboard/
│   ├── powerbi_dashboard.pbix      # Power BI file
│   └── dashboard_preview.png       # Dashboard screenshot
├── scripts/
│   └── data_cleaning.py     # Python script for ETL pipeline
└── README.md
