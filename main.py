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
# LEFT JOIN employees -> offices to include all employees, even without offices.
df_employee = pd.read_sql("""
    SELECT
        e.firstName,
        e.lastName,
        o.city,
        o.state
    FROM employees e
    LEFT JOIN offices o
        ON e.officeCode = o.officeCode
    ORDER BY
        e.firstName ASC,
        e.lastName ASC;
""", conn)

print("\n--- STEP 3: All employees with office city/state ---")
print(df_employee)

# STEP 4
# Find customers with no orders using LEFT JOIN and NULL filter.
df_contacts = pd.read_sql("""
    SELECT
        c.contactFirstName,
        c.contactLastName,
        c.phone,
        c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o
        ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY
        c.contactLastName ASC;
""", conn)

print("\n--- STEP 4: Customers with no orders ---")
print(df_contacts)

# STEP 5
# Return customer contacts with their payment amounts and dates.
# CAST amount as REAL so sorting works numerically (not lexicographically).
df_payment = pd.read_sql("""
    SELECT
        c.contactFirstName,
        c.contactLastName,
        p.paymentDate,
        p.amount,
        CAST(p.amount AS REAL) AS amount_numeric
    FROM customers c
    JOIN payments p
        ON c.customerNumber = p.customerNumber
    ORDER BY
        amount_numeric DESC;
""", conn)

print("\n--- STEP 5: Customer payments sorted by amount (descending) ---")
print(df_payment)

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