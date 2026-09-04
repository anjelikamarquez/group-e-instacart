# Instacart Data Engineering Project

## Project Overview

This project builds an end-to-end data engineering and analytics pipeline using the **Instacart Online Grocery Shopping dataset**.

The pipeline transforms raw CSV files through:

**Bronze → Data Profiling & Quality Checks → Silver → Gold → Analytics**

This notebook has a dedicated data profiling, anomaly-checking, and validation stage before the Silver layer.

The main goals are to:

- Ingest raw Instacart data into Databricks
- Profile and assess the quality of the raw data
- Detect missing values, duplicates, invalid values, and relationship issues
- Clean and validate the data in the Silver layer
- Build an analytics-ready Gold Star Schema
- Analyze product purchasing, purchasing time patterns, reordering, and customer loyalty

---

## Architecture

```text
                         RAW CSV FILES
                              │
                              ▼
                       ┌──────────────┐
                       │ BRONZE LAYER │
                       │ Raw ingestion│
                       └──────┬───────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ DATA PROFILING         │
                  │ • Row counts           │
                  │ • NULL analysis        │
                  │ • Distinct values      │
                  │ • Numeric ranges       │
                  └───────────┬────────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ DATA QUALITY VALIDATION│
                  │ • PK uniqueness        │
                  │ • NULL checks          │
                  │ • Duplicate rows       │
                  │ • Value/range checks   │
                  │ • Referential integrity│
                  │ • Anomaly check        │
                  └───────────┬────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │ SILVER LAYER │
                       │ Cleaned data │
                       └──────┬───────┘
                              │
                              ▼
                       ┌──────────────┐
                       │  GOLD LAYER  │
                       │ Star Schema  │
                       └──────┬───────┘
                              │
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
          DIM_ORDERS     DIM_PRODUCTS    FACT_ORDER_PRODUCTS
                              │
                              ▼
                       ┌──────────────┐
                       │  ANALYTICS   │
                       │ BI Questions │
                       └──────────────┘
```

---

# 1. Dataset

The project uses five source CSV files:

 Source File | Purpose 

- `aisles.csv` | Aisle/category lookup 
- `departments.csv` | Department lookup 
- `products.csv` | Product information and category IDs 
- `orders.csv` | Order-level and customer purchasing information 
- `order_products__prior.csv` | Products included in orders and reorder information 

### Source Row Counts

 Table | Rows 

- `aisles` | 134 
- `departments` | 21 
- `products` | 49,688 
- `orders` | 3,421,083 
- `order_products` | 32,434,489 

---

# 2. Bronze Layer

The Bronze layer contains the raw source data loaded into Databricks tables with explicit schemas.

Schema:

```text
workspace.instacart
├── aisles
├── departments
├── products
├── orders
└── order_products
```

The notebook uses Databricks SQL `read_files()` to read the CSV files and applies explicit column data types during ingestion.

### Bronze Tables

#### `aisles`

- `aisle_id` — INT
- `aisle` — STRING

#### `departments`

- `department_id` — INT
- `department` — STRING

#### `products`

- `product_id` — INT
- `product_name` — STRING
- `aisle_id` — INT
- `department_id` — INT

#### `orders`

- `order_id` — INT
- `user_id` — INT
- `eval_set` — STRING
- `order_number` — INT
- `order_dow` — INT
- `order_hour_of_day` — INT
- `days_since_prior_order` — DOUBLE

#### `order_products`

- `order_id` — INT
- `product_id` — INT
- `add_to_cart_order` — INT
- `reordered` — INT

---

# 3. Data Profiling

A dedicated profiling step was added before the Silver transformation.

A temporary `data_quality_report` view summarizes the initial state of the Bronze tables.

The profiling checks:

- Total row count
- NULL count
- NULL percentage
- Distinct value count
- Distinct percentage
- Data type
- Minimum value
- Maximum value

### Key Findings

| Field | Finding |
|---|---|
| `orders.order_id` | 3,421,083 unique values; 0 NULL |
| `products.product_id` | 49,688 unique values; 0 NULL |
| `orders.order_dow` | Range 0–6 |
| `orders.order_hour_of_day` | Range 0–23 |
| `order_products.reordered` | Values 0 or 1 |
| `orders.days_since_prior_order` | 206,209 NULL values (6.03%) |
| `products.aisle_id` | 1 NULL value |
| `products.department_id` | 1 NULL value |

The NULL values in `days_since_prior_order` are meaningful because they can indicate a customer's first recorded order.

---

# 4. Data Quality and Validation

The latest notebook contains five dedicated validation categories:

1. Primary key uniqueness
2. NULL values
3. Full-row duplicates
4. Value ranges and formats
5. Referential integrity

---

## 4.1 Primary Key Uniqueness

The following identifier columns were checked:

- `aisles.aisle_id`
- `departments.department_id`
- `products.product_id`
- `orders.order_id`

All primary key uniqueness checks passed.

| Table | Primary Key | Duplicate Count | Status |
|---|---|---:|---|
| `aisles` | `aisle_id` | 0 | PASS |
| `departments` | `department_id` | 0 | PASS |
| `products` | `product_id` | 0 | PASS |
| `orders` | `order_id` | 0 | PASS |

---

## 4.2 NULL Validation

Important columns were checked for missing values.

Most columns contain no NULLs.

Two product category IDs contain one NULL each:

- `products.aisle_id` → 1 NULL
- `products.department_id` → 1 NULL

These represent a very small portion of the product table and are retained.

The `orders.days_since_prior_order` field contains 206,209 NULL values, or 6.03%. These are treated as meaningful first-order indicators rather than automatically invalid records.

---

## 4.3 Full-Row Duplicate Validation

The notebook checks whether complete records are duplicated across all columns.

| Table | Duplicate Rows | Status |
|---|---:|---|
| `aisles` | 0 | PASS |
| `departments` | 0 | PASS |
| `products` | 0 | PASS |
| `orders` | 0 | PASS |
| `order_products` | 0 | PASS |

No full-row duplicates were detected.

---

## 4.4 Value Range and Format Validation

Business rules were used to confirm that values fall within expected ranges.

| Column | Expected Rule | Result |
|---|---|---|
| `order_dow` | 0–6 | PASS |
| `order_hour_of_day` | 0–23 | PASS |
| `order_number` | Positive integers | PASS |
| `days_since_prior_order` | Non-negative | PASS |
| `eval_set` | `prior`, `train`, `test` | PASS |
| `reordered` | 0 or 1 | PASS |
| `add_to_cart_order` | Positive integers | PASS |

No invalid values were detected.

---

## 4.5 Referential Integrity

The notebook checks whether foreign-key values have corresponding records.

### Passed

- `order_products.order_id` → `orders.order_id`
- `order_products.product_id` → `products.product_id`

Both relationships have **0 orphaned records**.

### Category relationship warnings

The following checks show one missing product category ID:

- `products.aisle_id` → `aisles.aisle_id`
- `products.department_id` → `departments.department_id`

This is caused by one product with a NULL `aisle_id` and one product with a NULL `department_id`.

The product records are retained, and missing category names are handled in the Gold layer.

---

# 5. Anomaly Check

The notebook also cross-checks:

- `days_since_prior_order`
- `reordered`

It specifically investigates records where:

```text
days_since_prior_order IS NULL
AND
reordered = 1
```

This combination may indicate an anomaly because a NULL `days_since_prior_order` can indicate a first order, while `reordered = 1` indicates that the product was previously purchased.

The notebook also recognizes that:

```text
days_since_prior_order IS NOT NULL
AND
reordered = 0
```

is not automatically an error. A customer can place another order without purchasing a previously purchased product.

---

# 6. Silver Layer

The Silver layer contains cleaned and validated versions of the Bronze tables.

Schema:

```text
workspace.instacart_silver
├── aisles
├── departments
├── products
├── orders
└── order_products
```

### Cleaning and Standardization

The Silver transformations include:

### String standardization

`TRIM()` removes unnecessary whitespace from text fields.

### NULL filtering

Required identifiers are checked so invalid records are not carried forward.

### Value-range enforcement

Expected values are enforced for:

- `eval_set`
- `order_dow`
- `order_hour_of_day`
- `reordered`
- `add_to_cart_order`
- `days_since_prior_order`

### Data type refinement

`days_since_prior_order` is converted from `DOUBLE` to `INT` because the values are whole numbers.

### Deduplication

`ROW_NUMBER()` with `QUALIFY` is used as a safety mechanism to maintain identifier uniqueness.

---

## Bronze vs Silver Row Counts

The row counts remained unchanged after the Silver transformations.

| Table | Bronze | Silver | Difference |
|---|---:|---:|---:|
| `aisles` | 134 | 134 | 0 |
| `departments` | 21 | 21 | 0 |
| `products` | 49,688 | 49,688 | 0 |
| `orders` | 3,421,083 | 3,421,083 | 0 |
| `order_products` | 32,434,489 | 32,434,489 | 0 |

This indicates that the current cleaning rules did not remove any records from the dataset.

---

# 7. Gold Layer

The Gold layer transforms the cleaned Silver tables into a simplified **Star Schema** for analytics.

Schema:

```text
workspace.instacart_gold
├── orders
├── products
└── order_products
```

## Star Schema

```text
                         ┌─────────────────┐
                         │     orders      │
                         │   DIMENSION     │
                         │                 │
                         │ PK order_id     │
                         │ user_id         │
                         │ eval_set        │
                         │ order_number    │
                         │ order_dow       │
                         │ order_hour...   │
                         │ days_since...   │
                         └────────┬────────┘
                                  │
                                  │ 1 : N
                                  ▼
                    ┌─────────────────────────┐
                    │     order_products      │
                    │          FACT           │
                    │                         │
                    │ FK order_id             │
                    │ FK product_id           │
                    │ add_to_cart_order       │
                    │ reordered               │
                    └────────────┬────────────┘
                                 │
                                 │ N : 1
                                 ▼
                         ┌─────────────────┐
                         │     products    │
                         │    DIMENSION    │
                         │                 │
                         │ PK product_id   │
                         │ product_name    │
                         │ aisle_id        │
                         │ aisle           │
                         │ department_id   │
                         │ department      │
                         └─────────────────┘
```

---

## 7.1 Fact Table: `order_products`

### Grain

> **One row represents one product within one order.**

Columns:

- `order_id`
- `product_id`
- `add_to_cart_order`
- `reordered`

Foreign keys:

- `order_id` → `orders.order_id`
- `product_id` → `products.product_id`

---

## 7.2 Dimension Table: `orders`

The order dimension contains order-level, customer, and time context.

Columns:

- `order_id`
- `user_id`
- `eval_set`
- `order_number`
- `order_dow`
- `order_hour_of_day`
- `days_since_prior_order`

Primary key:

```text
order_id
```

---

## 7.3 Dimension Table: `products`

The product dimension combines:

- Product information
- Aisle information
- Department information

The product table is joined with the aisle and department tables so that descriptive category names are available directly in the dimension.

For missing aisle or department names, the Gold layer uses:

```sql
COALESCE(..., 'Unknown')
```

This keeps the product record available for analytics even when category descriptions are missing.

Primary key:

```text
product_id
```

---

# 8. Gold Layer Validation

The Gold layer was validated after creation.

### Row Counts

| Table | Rows |
|---|---:|
| `orders` | 3,421,083 |
| `products` | 49,688 |
| `order_products` | 32,434,489 |

### Star Schema Relationship Checks

| Relationship | Orphaned Records |
|---|---:|
| `order_products.order_id` → `orders.order_id` | 0 |
| `order_products.product_id` → `products.product_id` | 0 |

This confirms that all fact-table order and product IDs have matching dimension records.

---

# 9. Business Intelligence Analysis

The Gold Star Schema is used to answer four business questions.

---

## Q1. Which products and departments are purchased most frequently?

### Product analysis

The query measures:

- Number of orders containing each product
- Total units purchased
- Reorder rate
- Department
- Aisle

The **top 20 products by order frequency** are returned.

### Department analysis

Departments are compared using:

- Orders containing the department
- Total products sold
- Unique products
- Average cart position
- Reorder rate

---

## Q2. How does customer purchasing behavior change by day of week and hour of day?

The analysis uses three views.

### Day of Week

Metrics include:

- Total orders
- Unique customers
- Total products
- Average basket size
- Reorder rate

The numeric day values are converted to:

```text
0 = Sunday
1 = Monday
2 = Tuesday
3 = Wednesday
4 = Thursday
5 = Friday
6 = Saturday
```

### Hour of Day

Orders are analyzed from hour 0 through hour 23.

The hours are grouped into:

- Night: 12am–6am
- Morning: 6am–12pm
- Afternoon: 12pm–6pm
- Evening: 6pm–12am

### Day × Hour

A separate query provides day-and-hour data for a heatmap.

Recommended visualizations:

- Combo chart: order volume + reorder rate by day
- Combo chart: order volume + reorder rate by hour
- Heatmap: order activity by day and hour

---

## Q3. Which products and departments have the highest reorder behavior?

The product-level analysis measures:

- Total units sold
- Times reordered
- Reorder rate
- Unique orders

A minimum of **100 unique orders** is required before a product is considered for the top reorder-rate analysis.

The notebook returns the **top 10 products** by reorder rate.

Department-level reorder behavior is also analyzed using:

- Total units sold
- Times reordered
- Reorder rate
- Unique products
- Unique orders

---

## Q4. What is the customer loyalty profile?

Customers are segmented using:

- Total number of orders
- Reorder rate

### Customer Segments

| Segment | Rule |
|---|---|
| **VIP Loyalist** | 50+ orders AND 60%+ reorder rate |
| **Loyal Regular** | 30+ orders AND 50%+ reorder rate |
| **Regular Customer** | 15+ orders |
| **Occasional Shopper** | 5+ orders |
| **New Customer** | Fewer than 5 orders |

### Customer Segment Results

| Segment | Customers | Avg Orders | Avg Basket | Avg Reorder Rate | Product Volume Share |
|---|---:|---:|---:|---:|---:|
| VIP Loyalist | 9,421 | 69.5 | 10.14 | 77.08% | 21.49% |
| Loyal Regular | 16,720 | 39.2 | 10.68 | 70.57% | 22.87% |
| Regular Customer | 39,155 | 21.4 | 10.06 | 59.42% | 27.48% |
| Occasional Shopper | 81,172 | 8.5 | 9.94 | 47.02% | 22.69% |
| New Customer | 59,741 | 2.9 | 9.67 | 34.33% | 5.47% |

> **Note:** The notebook column is named `pct_total_revenue`, but its calculation is based on `total_products`, not monetary revenue. It should therefore be interpreted as **percentage of total product volume**, not actual revenue.

---

## Department Preferences by Customer Segment

The notebook also identifies the top five departments purchased by each customer segment.

The leading departments across the segments include:

1. Produce
2. Dairy & Eggs
3. Snacks
4. Beverages
5. Frozen

This provides additional insight into the product preferences of different customer loyalty groups.

---

# 10. Data Quality Summary

The latest version of the notebook provides a more comprehensive data-quality process than the earlier version.

### Validation Summary

- Primary key uniqueness: **PASS**
- Full-row duplicate checks: **PASS**
- Value/range checks: **PASS**
- Fact → Order referential integrity: **PASS**
- Fact → Product referential integrity: **PASS**
- Bronze → Silver row counts: **unchanged**
- Product category IDs: **1 NULL aisle ID and 1 NULL department ID**
- `days_since_prior_order`: **206,209 NULL values (6.03%)**, treated as meaningful first-order values

The small number of missing product category IDs is documented and handled in the Gold product dimension with `Unknown` descriptive values.

---

# 11. Technology Stack

- **Databricks**
- **Databricks SQL**
- SQL `read_files()`
- Common Table Expressions (CTEs)
- Window functions
- `ROW_NUMBER()`
- `QUALIFY`
- `CASE`
- `COALESCE`
- Aggregation functions
- Star Schema / dimensional modeling
- Databricks visualizations

---

# 12. Project Workflow

```text
1. Load raw CSV files
        ↓
2. Create Bronze tables
        ↓
3. Profile Bronze data
        ↓
4. Check NULLs, duplicates and ranges
        ↓
5. Investigate potential anomalies
        ↓
6. Create cleaned Silver tables
        ↓
7. Compare Bronze vs Silver row counts
        ↓
8. Validate data quality
        ↓
9. Create Gold Star Schema
        ↓
10. Validate Star Schema relationships
        ↓
11. Run business intelligence queries
        ↓
12. Visualize purchasing and customer behavior
```

---

# 13. Key Takeaways

This project demonstrates an end-to-end data engineering workflow covering:

### Data Engineering

- Raw CSV ingestion
- Schema definition
- Data profiling
- Data-quality validation
- Anomaly checking
- Data cleaning
- Deduplication
- Data type refinement
- Referential integrity checks

### Data Modeling

- Fact and dimension identification
- Grain definition
- Primary and foreign keys
- One-to-many relationships
- Star Schema design
- Product dimension denormalization

### Analytics

- Product and department purchasing frequency
- Purchasing behavior by day and hour
- Reorder behavior
- Customer loyalty segmentation
- Department preferences by customer segment

The final Gold layer provides a simplified, validated, and analytics-ready structure for BI reporting and visualization.

---

# Repository Structure

A recommended GitHub repository structure is:

```text
instacart-data-engineering/
│
├── README.md
│
├── notebooks/
│   └── Group E_Instacart Notebook (3).ipynb
│
└── docs/
    ├── ARCHITECTURE.md
    ├── MODEL.md
    └── ANALYTICS.md
```

---

## Project Status

**Completed:** Bronze, Silver, and Gold layers, data profiling and validation, Star Schema modeling, Star Schema relationship validation, and business intelligence analyses.

The current notebook represents the latest documented version of the Instacart data engineering pipeline.
