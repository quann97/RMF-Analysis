from datetime import datetime, timedelta, date
import pandas as pd
import pyodbc 

# =============================================================================
# Connect to SQL Server
# =============================================================================
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-RFH0PE8\SQLEXPRESS;'
                      'Database=Superstore;'
                      'Trusted_Connection=yes;')

# =============================================================================
# Transform Data with SQL Server
# =============================================================================
df_RFM = pd.read_sql_query('''WITH RFM_Base 
    AS
    (
        SELECT a.Customer_ID, b.Customer_Name AS CustomerName,
        DATEDIFF(DAY, MAX(a.Order_Date), CONVERT(DATE, GETDATE())) AS Recency_Value,
        COUNT(DISTINCT a.Order_Date) AS Frequency_Value,
        ROUND(SUM(a.Sales), 2) AS Monetary_Value
      FROM sales AS a
      INNER JOIN customers AS b ON a.Customer_ID = b.Customer_ID
      GROUP BY a.Customer_ID, b.Customer_Name
    )
    -- SELECT * FROM RFM_Base
    , RFM_Score 
    AS
    (
      SELECT *,
        NTILE(5) OVER (ORDER BY Recency_Value DESC) as R_Score,
        NTILE(5) OVER (ORDER BY Frequency_Value ASC) as F_Score,
        NTILE(5) OVER (ORDER BY Monetary_Value ASC) as M_Score
      FROM RFM_Base
    )
    -- SELECT * FROM RFM_Score
    , RFM_Final
    AS
    (
    SELECT *,
      CONCAT(R_Score, F_Score, M_Score) as RFM_Overall
      -- , (R_Score + F_Score + M_Score) as RFM_Overall1
      -- , CAST(R_Score AS char(1))+CAST(F_Score AS char(1))+CAST(M_Score AS char(1)) as RFM_Overall2
    FROM RFM_Score
    )
    -- SELECT * FROM RFM_Final
    SELECT f.*, s.Segment
    FROM RFM_Final f
    JOIN [segment scores] s ON f.RFM_Overall = s.Scores
    ; ''', conn)

# =============================================================================
# Transform Data with Python
# =============================================================================

df_sales = pd.read_sql_query('SELECT * FROM sales', conn)
df_customers = pd.read_sql_query('SELECT * FROM customers', conn)
df_segmentScores = pd.read_sql_query('SELECT * FROM [segment scores]', conn)

df_sales.info()
df_customers.info()
df_segmentScores.info()

df_RFM.info()
df_RFM['CustomerName'].nunique()
# =============================Recency=========================================
 
df_sales['Order_Date'] = pd.to_datetime(df_sales['Order_Date'])

df_user = df_customers[['Customer_ID','Customer_Name']]
df_user.columns = ['CustomerID','CustomerName']

df_maxPurchase = df_sales.groupby('Customer_ID').Order_Date.max().reset_index()
df_maxPurchase.columns = ['CustomerID','MaxPurchaseDate']

df_maxPurchase['Recency Value'] = (datetime.now() - df_maxPurchase['MaxPurchaseDate']).dt.days

df_RFMOverall = pd.merge(df_user, df_maxPurchase[['CustomerID','Recency Value']], on='CustomerID')

df_RFMOverall['Recency Value'].describe()

# =============================Frequency=======================================

df_frequency = df_sales.groupby('Customer_ID').Order_Date.nunique().reset_index()
df_frequency.columns = ['CustomerID','Frequency Value']

df_RFMOverall = pd.merge(df_RFMOverall, df_frequency, on='CustomerID')

df_RFMOverall['Frequency Value'].describe()

# =============================Monetery Value==================================

df_monetery = df_sales.groupby('Customer_ID').Sales.sum().reset_index()

df_monetery.columns = ['CustomerID','Monetery Value']

df_RFMOverall = pd.merge(df_RFMOverall, df_monetery, on='CustomerID')

df_RFMOverall['Monetery Value'].describe()

# =============================RFM-Score==================================

df_RFMOverall['R-Score'] = pd.qcut(df_RFMOverall['Recency Value'], 5, labels=[5,4,3,2,1]).astype(str)
df_RFMOverall['F-Score'] = pd.qcut(df_RFMOverall['Frequency Value'], 5, labels=[1,2,3,4,5]).astype(str)
df_RFMOverall['M-Score'] = pd.qcut(df_RFMOverall['Monetery Value'], 5, labels=[1,2,3,4,5]).astype(str)

df_RFMOverall['RFM_Overall'] = df_RFMOverall['R-Score'] + df_RFMOverall['F-Score'] + df_RFMOverall['M-Score']

df_RFMOverall.info()
df_segmentScores.info()

# =============================Segment name==================================

df_segmentScores['Scores'] = df_segmentScores['Scores'].astype(str)

df_RFMOverall = df_RFMOverall.merge(df_segmentScores, how='inner', left_on = 'RFM_Overall', right_on = 'Scores')

df_RFMOverall = df_RFMOverall.iloc[:, :-1]
# df_RFMOverall.drop('Scores', axis=1, inplace=True)








