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
# Identify employees whose customers have avg credit limit > 90000.
# Return employee info and customer count. Sort by number of customers desc.
df_credit = pd.read_sql("""
    SELECT
        e.employeeNumber,
        e.firstName,
        e.lastName,
        COUNT(c.customerNumber) AS num_customers
    FROM employees e
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY
        e.employeeNumber,
        e.firstName,
        e.lastName
    HAVING
        AVG(c.creditLimit) > 90000
    ORDER BY
        num_customers DESC;
""", conn)

print("\n--- STEP 6: Employees with avg customer credit limit > 90000 ---")
print(df_credit)

# STEP 7
# Determine best-selling products by total quantity ordered and number of orders.
df_product_sold = pd.read_sql("""
    SELECT
        p.productName,
        COUNT(od.orderNumber) AS numorders,
        SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od
        ON p.productCode = od.productCode
    GROUP BY
        p.productName
    ORDER BY
        totalunits DESC;
""", conn)

print("\n--- STEP 7: Top-selling products by total units ---")
print(df_product_sold)

# STEP 8
# Determine number of unique customers per product (market reach).
df_total_customers = pd.read_sql("""
    SELECT
        p.productName,
        p.productCode,
        COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od
        ON p.productCode = od.productCode
    JOIN orders o
        ON od.orderNumber = o.orderNumber
    GROUP BY
        p.productName,
        p.productCode
    ORDER BY
        numpurchasers DESC;
""", conn)

print("\n--- STEP 8: Unique customer count per product ---")
print(df_total_customers)

# STEP 9
# Determine number of customers per office location.
df_customers = pd.read_sql("""
    SELECT
        o.officeCode,
        o.city,
        COUNT(DISTINCT c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e
        ON o.officeCode = e.officeCode
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY
        o.officeCode,
        o.city
    ORDER BY
        n_customers DESC;
""", conn)

print("\n--- STEP 9: Customer count per office ---")
print(df_customers)

# STEP 10
# Find employees who sold products ordered by fewer than 20 unique customers.
# Use a subquery to identify underperforming products (fewer than 20 distinct customers).

df_under_20 = pd.read_sql("""
    SELECT
        e.employeeNumber,
        e.firstName,
        e.lastName,
        o.city,
        o.officeCode
    FROM employees e
    JOIN offices o
        ON e.officeCode = o.officeCode
    JOIN customers c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord
        ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od
        ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        SELECT
            p.productCode
        FROM products p
        JOIN orderdetails od2
            ON p.productCode = od2.productCode
        JOIN orders ord2
            ON od2.orderNumber = ord2.orderNumber
        GROUP BY
            p.productCode
        HAVING
            COUNT(DISTINCT ord2.customerNumber) < 20
    )
    GROUP BY
        e.employeeNumber,
        e.firstName,
        e.lastName,
        o.city,
        o.officeCode
    ORDER BY
        e.employeeNumber;
""", conn)

print("\n--- STEP 10: Employees who sold products with fewer than 20 customers ---")
print(df_under_20)