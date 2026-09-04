# Instacart Data Engineering Pipeline & Analytics

## Project Overview

This project builds an end-to-end data engineering pipeline for the
**Instacart** dataset using **Databricks SQL**.

The pipeline follows the **Medallion Architecture**:

``` text
Raw CSV Files
     │
     ▼
  BRONZE
Raw ingestion
     │
     ▼
  SILVER
Cleaning + validation
     │
     ▼
   GOLD
Star schema
     │
     ▼
Analytics
```

The main goal is to transform raw Instacart order data into a clean,
analytics-ready **Star Schema**, then use SQL to answer business
questions about purchasing patterns, product popularity, reorder
behavior, and customer loyalty.

------------------------------------------------------------------------

## Technologies Used

-   **Databricks**
-   **Databricks SQL**
-   **Delta Tables**
-   **Medallion Architecture**
-   **Star Schema / Dimensional Modeling**
-   **GitHub** for documentation and version control

------------------------------------------------------------------------

## Dataset

The project uses the following Instacart CSV files:

  Source File                   Description
  ----------------------------- --------------------------------------
  - `aisles.csv`                  Product aisle reference data
  -  `departments.csv`             Product department reference data
  - `products.csv`                Product information and category IDs
  - `orders.csv`                  Order-level information
  - `order_products__prior.csv`   Products included in prior orders

The files were ingested from a Databricks Volume into the Bronze layer.

### Dataset Size

  Table                    Rows
  ---------------- ------------
  Aisles                    134
  Departments                21
  Products               49,688
  Orders              3,421,083
  Order Products     32,434,489

------------------------------------------------------------------------

# 1. Bronze Layer

## Purpose

The Bronze layer stores the source data with minimal transformation.

The CSV files are loaded into the `workspace.instacart` schema as Delta
tables.

### Bronze Tables

``` text
workspace.instacart
├── aisles
├── departments
├── products
├── orders
└── order_products
```

### Ingestion

The pipeline creates the schema if it does not exist and reads each CSV
file using `read_files()`.

Example:

``` sql
CREATE SCHEMA IF NOT EXISTS workspace.instacart;

CREATE OR REPLACE TABLE instacart.aisles AS
SELECT *
FROM read_files(
    '/Volumes/workspace/default/ftw-b12/shared/week06/instacart_csv/aisles.csv',
    format => 'csv',
    header => true,
    schema => 'aisle_id INT, aisle STRING'
);
```

Explicit schemas are used during ingestion so that important columns
receive the expected data types.

------------------------------------------------------------------------

# 2. Silver Layer

## Purpose

The Silver layer contains cleaned and validated versions of the Bronze
data.

Schema:

``` text
workspace.instacart_silver
├── aisles
├── departments
├── products
├── orders
└── order_products
```

## Cleaning and Validation

The following transformations were applied:

### String standardization

Whitespace was removed from text fields using `TRIM()`.

``` sql
TRIM(product_name)
TRIM(aisle)
TRIM(department)
```

### Null validation

Required identifier columns were filtered to remove null IDs.

Examples:

``` sql
WHERE product_id IS NOT NULL
```

``` sql
WHERE order_id IS NOT NULL
  AND user_id IS NOT NULL
```

### Value validation

Business rules were applied to fields with known valid ranges.

-   `eval_set` must be `prior`, `train`, or `test`
-   `order_dow` must be between 0 and 6
-   `order_hour_of_day` must be between 0 and 23
-   `reordered` must be 0 or 1
-   `add_to_cart_order` must be greater than 0

### Data type refinement

`days_since_prior_order` was converted from `DOUBLE` to `INT` because
the values are whole numbers.

``` sql
CAST(days_since_prior_order AS INT)
```

### Deduplication

`ROW_NUMBER()` with `QUALIFY` was used to ensure unique identifiers in
the dimension tables.

Example:

``` sql
QUALIFY ROW_NUMBER()
    OVER (PARTITION BY product_id ORDER BY product_name) = 1
```

### Referential integrity validation

The `order_products` table was validated against the Silver `orders` and
`products` tables using `INNER JOIN`.

This removes orphaned fact records if an order or product does not exist
in its corresponding dimension.

For this dataset, no orphaned records were found.

------------------------------------------------------------------------

## Bronze vs Silver Validation

The row counts remained unchanged after cleaning:

  Table                  Bronze       Silver
  ---------------- ------------ ------------
  Aisles                    134          134
  Departments                21           21
  Products               49,688       49,688
  Orders              3,421,083    3,421,083
  Order Products     32,434,489   32,434,489

This indicates that the validation rules did not remove any rows from
this particular dataset.

One product (`product_id = 6816`) has null `aisle_id` and
`department_id`. It was retained in Silver to preserve the original
product record.

------------------------------------------------------------------------

# 3. Gold Layer

## Purpose

The Gold layer converts the cleaned Silver data into an analytics-ready
**Star Schema**.

Schema:

``` text
workspace.instacart_gold
├── orders
├── products
└── order_products
```

## Star Schema

``` text
                    ┌──────────────────────┐
                    │       orders         │
                    │      DIMENSION       │
                    │----------------------│
                    │ PK order_id          │
                    │ user_id              │
                    │ eval_set             │
                    │ order_number         │
                    │ order_dow            │
                    │ order_hour_of_day    │
                    │ days_since_prior...  │
                    └──────────┬───────────┘
                               │
                               │ 1 : N
                               │
                               ▼
                    ┌──────────────────────┐
                    │   order_products     │
                    │        FACT          │
                    │----------------------│
                    │ FK order_id         │
                    │ FK product_id       │
                    │ add_to_cart_order   │
                    │ reordered           │
                    └──────────▲───────────┘
                               │
                               │ N : 1
                               │
                    ┌──────────┴───────────┐
                    │       products       │
                    │      DIMENSION       │
                    │----------------------│
                    │ PK product_id        │
                    │ product_name         │
                    │ aisle_id             │
                    │ aisle                │
                    │ department_id        │
                    │ department           │
                    └──────────────────────┘
```

### Fact Table

`order_products` is the fact table.

**Grain:**

> One row represents one product within one order.

Columns:

  Column                Role
  --------------------- ------------------------------
  `order_id`            Foreign key to `orders`
  `product_id`          Foreign key to `products`
  `add_to_cart_order`   Product position in the cart
  `reordered`           Reorder indicator

### Dimension Tables

#### `orders`

Contains order-level context such as:

-   Customer/user ID
-   Order sequence
-   Evaluation set
-   Day of week
-   Hour of day
-   Days since previous order

#### `products`

Contains product information and denormalized category information.

The `products`, `aisles`, and `departments` Silver tables are combined
so that the Gold product dimension contains:

-   Product name
-   Aisle ID and aisle name
-   Department ID and department name

This reduces the number of joins required by downstream analytics
queries.

------------------------------------------------------------------------

# 4. Primary Keys and Foreign Keys

The Gold model uses the following relationships:

  -----------------------------------------------------------------------
  Table                   Column                  Role
  ----------------------- ----------------------- -----------------------
  `orders`                `order_id`              Primary Key

  `products`              `product_id`            Primary Key

  `order_products`        `order_id`              Foreign Key →
                                                  `orders.order_id`

  `order_products`        `product_id`            Foreign Key →
                                                  `products.product_id`
  -----------------------------------------------------------------------

### Cardinality

``` text
orders
  1
  │
  │
  N
order_products
  N
  │
  │
  1
products
```

Business interpretation:

-   One order can contain many products.
-   One product can appear in many orders.

The PK/FK relationships are also defined in Databricks so the
relationships can be viewed through the Databricks ERD.

------------------------------------------------------------------------

# 5. Gold Layer Validation

The Gold layer contains:

  Table                      Rows
  ------------------ ------------
  `orders`              3,421,083
  `products`               49,688
  `order_products`     32,434,489

Referential integrity was checked for both foreign keys.

  -----------------------------------------------------------------------
  Foreign Key         Distinct Fact Distinct Dimension   Orphaned Records
                             Values             Values 
  -------------- ------------------ ------------------ ------------------
  `order_id`              3,214,874          3,214,874                  0

  `product_id`               49,677             49,677                  0
  -----------------------------------------------------------------------

Both foreign key checks returned **0 orphaned records**.

------------------------------------------------------------------------

# 6. Business Intelligence & Analytics

The Gold Star Schema is used to answer four main business questions.

## Q1. Which products and departments are purchased most frequently?

### Product analysis

The query calculates:

-   Number of orders containing the product
-   Total units
-   Reorder rate
-   Product department
-   Product aisle

The analysis is limited to the top 20 products by order frequency.

### Example result

The most frequently ordered product in the query results was:

**Banana**

It appeared in approximately **472,565 orders** with an **84.35% reorder
rate**.

Other highly ordered products included:

-   Bag of Organic Bananas
-   Organic Strawberries
-   Organic Baby Spinach
-   Organic Hass Avocado

### Department analysis

The department query measures:

-   Orders containing the department
-   Total products sold
-   Number of unique products
-   Average cart position
-   Reorder rate

The highest-volume department was **produce**, followed by **dairy
eggs** and **beverages**.

------------------------------------------------------------------------

# 7. Purchasing Behavior by Day and Time

## Q2. How does customer purchasing behavior change by day of week and hour of day?

The analysis examines:

-   Total orders
-   Unique customers
-   Total products
-   Average basket size
-   Reorder rate

### Day of Week

Orders were analyzed using the `order_dow` field:

``` text
0 = Sunday
1 = Monday
2 = Tuesday
3 = Wednesday
4 = Thursday
5 = Friday
6 = Saturday
```

Sunday had the highest number of orders in the dataset, with
approximately **557,772 orders**.

The query also calculates average basket size and reorder rate for each
day.

### Hour of Day

Orders were analyzed across hours 0--23.

The results can be used to identify high-traffic purchasing periods and
differences in basket size and reorder behavior.

### Heatmap Dataset

A separate query produces a:

``` text
Day of Week × Hour of Day
```

dataset containing:

-   Total orders
-   Average basket size

This output can be used to create a heatmap visualization.

------------------------------------------------------------------------

# 8. Reorder Behavior

## Q3. Which products have the highest reorder behavior?

The analysis calculates:

-   Total units sold
-   Times reordered
-   Reorder rate
-   Unique orders

A minimum threshold of **100 unique orders** is applied to avoid ranking
products based on very small sample sizes.

The top results include products with reorder rates above 85%.

At the department level, **dairy eggs** had the highest reorder rate at
approximately **67.00%**, followed by:

-   Beverages --- 65.35%
-   Produce --- 64.99%
-   Bakery --- 62.81%
-   Deli --- 60.77%

This indicates that frequently purchased grocery categories tend to have
stronger repeat-purchase behavior.

------------------------------------------------------------------------

# 9. Customer Loyalty Analysis

## Q4. What is the customer loyalty profile?

Customers are segmented based on:

-   Total orders
-   Reorder rate
-   Average basket size
-   Average days between orders
-   Customer tenure in orders

### Customer Segments

  Segment              Definition
  -------------------- ----------------------------------
  VIP Loyalist         50+ orders and 60%+ reorder rate
  Loyal Regular        30+ orders and 50%+ reorder rate
  Regular Customer     15+ orders
  Occasional Shopper   5+ orders
  New Customer         Fewer than 5 orders

### Observed customer profile

The query results show that:

-   **VIP Loyalists** average 69.5 orders and have a 77.08% average
    reorder rate.
-   **Loyal Regulars** average 39.2 orders and have a 70.57% average
    reorder rate.
-   **Regular Customers** average 21.4 orders.
-   **Occasional Shoppers** average 8.5 orders.
-   **New Customers** average 2.9 orders.

The VIP Loyalist group represents approximately **21.49% of total
product volume**, while Loyal Regulars represent approximately
**22.87%**.

> Note: The original SQL column is named `pct_total_revenue`, but it is
> calculated from `total_products`, not monetary revenue. It should be
> interpreted as **percentage of total product volume**, not revenue.

------------------------------------------------------------------------

# 10. Data Quality Checks

The project includes validation at multiple stages.

### Bronze → Silver

Checks include:

-   Null identifiers
-   Valid categorical values
-   Valid ranges
-   Duplicate IDs
-   Foreign key relationships
-   Data type consistency

### Silver → Gold

Checks include:

-   Gold table row counts
-   Primary key uniqueness
-   Foreign key integrity
-   Orphan detection

The final Gold model passed the foreign key integrity checks with:

``` text
order_id orphaned records  = 0
product_id orphaned records = 0
```

------------------------------------------------------------------------

# 11. Project Structure

A recommended GitHub repository structure is:

``` text
instacart-data-engineering/
│
├── README.md
│
├── notebooks/
│   └── Group E_Instacart Notebook_v2.ipynb
│
├── sql/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── analytics/
│
└── diagrams/
    └── instacart-star-schema-erd.png
```

If the project is maintained directly as a Databricks notebook, the
notebook can remain the main implementation artifact while the README
serves as the project documentation.

------------------------------------------------------------------------

# 12. End-to-End Workflow

The complete workflow is:

``` text
1. Load CSV files
       ↓
2. Create Bronze tables
       ↓
3. Clean and validate data
       ↓
4. Create Silver tables
       ↓
5. Validate Bronze vs Silver
       ↓
6. Build Gold Star Schema
       ↓
7. Define PK/FK relationships
       ↓
8. Validate referential integrity
       ↓
9. Run business intelligence queries
       ↓
10. Analyze purchasing and customer behavior
```

------------------------------------------------------------------------

# 13. Key Outcomes

This project demonstrates the following data engineering concepts:

-   Raw data ingestion
-   Explicit schema definition
-   Medallion Architecture
-   Data cleaning and standardization
-   Data validation
-   Deduplication
-   Referential integrity
-   Primary and foreign keys
-   Dimensional modeling
-   Star Schema design
-   Fact and dimension tables
-   Grain definition
-   SQL transformations
-   Business intelligence analysis
-   Customer segmentation
-   Data quality testing
-   Databricks ERD visualization

The final result is a clean, validated, analytics-ready Star Schema that
can support BI reporting and further analysis.

------------------------------------------------------------------------

# 14. Known Issue

The final **Department Preferences by Customer Segment** query in the
notebook currently returns an `AnalysisException` in its saved notebook
output.

The intended analysis is to identify the top five departments purchased
by each customer segment.

This query should be fixed and successfully rerun before treating that
output as a final project result.

A further improvement would be to rename the `pct_total_revenue` field
in the customer segmentation query to something such as:

``` text
pct_total_product_volume
```

because the current calculation is based on product counts and does not
use revenue data.

------------------------------------------------------------------------

## Conclusion

The Instacart project demonstrates a complete analytics pipeline in
Databricks, starting from raw CSV ingestion and progressing through data
cleaning, validation, dimensional modeling, and business analysis.

The Gold layer provides a simple three-table Star Schema:

``` text
        DIM_ORDERS
             │
             │ 1:N
             ▼
      FACT_ORDER_PRODUCTS
             ▲
             │ N:1
             │
        DIM_PRODUCTS
```

This structure makes the dataset easier to query and provides a strong
foundation for BI dashboards, customer behavior analysis, product
analysis, and future machine learning use cases.
[README.md](https://github.com/user-attachments/files/31786336/README.md)
