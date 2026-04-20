# vendor_performance_analysis
This project identifies underperforming brands that require promotional or pricing adjustments and investigates profitability variance between high- and low-performing vendors using Python, MySQL, and Power BI.

# DATASET
- [x] Source:
- [x] Size: 10,692 rows x 14 columns
- [x] Description:

# TOOLS & TECHNOLOGIES
- [x] Python (Pandas, SQLalchemy) - Data loading, exploratory data analysis (EDA), and data cleaning
- [x] SQL (MySQL) - Querying and insights generation
- [x] Power BI - Dashboard construction and data visualization
- [x] Github - Project sharing

# DATA PREPARATION IN PYTHON (JUPYTER NOTEBOOK)
- [x] Loaded raw data from CSV files and ingested it into the 'vendors_performance' database.
- [x] Explored schema with .info() and .describe()
- [x] Created summary table 'vendors_sales_summary', to compact our important columns and computed values into one. Made it more consistent and time-effective for data visualization in Power BI.

# EXPLORATORY DATA ANALYSIS (EDA)
- [x] Conducted EDA using the table, 'vendor_sales_summary'.
- [x] Distribution plots such as histograms, bar plots, circle plots, and scatter plots were created to visualize different distributions (after data was cleaned).

# DATA CLEANING
- [x] Handled null values
- [x] Removed duplicates
- [x] Removed rows where numerical values in columns of 'GrossProfit' and 'ProfitMargins' were less than (<) 0
- [x] Made each numerical column more readable by limiting the values to 2 (.2f) decimal values

# SQL QUERIES
- [x] Filtered data to remove inconsistencies
- [x] Ran queries for other insights

# POWER BI DASHBOARD
- [x] Designed an interactive dashboard with charts (i.e., bar, circle) and cards.
# DASHBOARD & RESULTS
- [x] Key KPIs visualized (e.g., purchase contribution %, low-performing vendors, and top vendors by sales/brands
