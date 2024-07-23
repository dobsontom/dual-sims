# Dual SIMs Data Processing

## Overview
This repository contains a SQL query that processes dual SIM data for maritime users. The query identifies pairs of SIM cards with close temporal and spatial proximity within specified date ranges. It is run on the first and ninth of every month to ensure up-to-date data. The data is subsequently visualised in a Tableau dashboard.

## Scheduled Query

| Query                       | Schedule                             |
|-----------------------------|--------------------------------------|
| `dual_sims_processing`      | Monthly on the 1st and 9th           |

## Tables Used

### Source Tables

- `inm-iar-data-warehouse-dev.commercial_product.commercial_product__monthly_view`
- `inm-iar-data-warehouse-dev.commercial_product.dim_instance_products`
- `inm-iar-data-warehouse-dev.commercial_product.dim_instance_customers`
- `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`

### Generated Table

- `inm-iar-data-warehouse-dev.td_dual_sims.dual_sims`

## Query Details

### Key Components

1. **Date Ranges**
   - Generates start and end dates for three 10-day periods within the last six months.

2. **Variables**
   - Captures the current month minus one month for reference.

3. **Dual SIM Groups**
   - Identifies groups of dual SIM cards active within the specified date range.

4. **IMSI List**
   - Lists IMSI details for the identified dual SIM groups.

5. **Data Ranges**
   - Extracts position data for the identified date ranges (1-10, 11-20, 21-31 of each month).

6. **Position Data**
   - Calculates the spatial and temporal proximity between pairs of SIM cards within each date range.

7. **Combined Data**
   - Combines position data from all date ranges into a single dataset.

8. **Final Select**
   - Outputs the final table with calculated distances and time differences between pairs of SIM cards.

---

