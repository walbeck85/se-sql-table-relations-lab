# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# Inspect schema so we can confirm table and column names while we work
print("\n--- SCHEMA INSPECTION ---")
print(pd.read_sql("""
    SELECT name, sql
    FROM sqlite_master
    WHERE type='table';
""", conn))

# STEP 1
# Return first/last name and job title for all employees in Boston.
# JOIN employees -> offices and filter with WHERE on city = 'Boston'.
df_boston = pd.read_sql("""
    SELECT
        e.firstName,
        e.lastName,
        e.jobTitle
    FROM employees e
    JOIN offices o
        ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston';
""", conn)

print("\n--- STEP 1: Employees in Boston ---")
print(df_boston)

# STEP 2
# Find offices that have zero employees.
# LEFT JOIN offices -> employees, GROUP BY office, HAVING COUNT(...) = 0.
df_zero_emp = pd.read_sql("""
    SELECT
        o.officeCode,
        o.city,
        COUNT(e.employeeNumber) AS num_employees
    FROM offices o
    LEFT JOIN employees e
        ON o.officeCode = e.officeCode
    GROUP BY
        o.officeCode,
        o.city
    HAVING COUNT(e.employeeNumber) = 0;
""", conn)

print(df_boston, flush=True)
print(df_zero_emp, flush=True)

# STEP 3
# Replace None with your code
df_employee = None

# STEP 4
# Replace None with your code
df_contacts = None

# STEP 5
# Replace None with your code
df_payment = None

# STEP 6
# Replace None with your code
df_credit = None

# STEP 7
# Replace None with your code
df_product_sold = None

# STEP 8
# Replace None with your code
df_total_customers = None

# STEP 9
# Replace None with your code
df_customers = None

# STEP 10
# Replace None with your code
df_under_20 = None

conn.close()