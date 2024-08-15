CREATE OR REPLACE TABLE `inm-iar-data-warehouse-dev.td_dual_sims.dual_sims` AS (
    WITH
        dates AS (
            SELECT
                month_start_date,
                DATE_ADD(month_start_date, INTERVAL 9 DAY) AS p1_end_date,
                DATE_ADD(month_start_date, INTERVAL 10 DAY) AS p2_start_date,
                DATE_ADD(month_start_date, INTERVAL 19 DAY) AS p2_end_date,
                DATE_ADD(month_start_date, INTERVAL 20 DAY) AS p3_start_date,
                LAST_DAY(month_start_date, MONTH) AS month_end_date
            FROM
                (
                    SELECT
                        *
                    FROM
                        UNNEST (
                            GENERATE_DATE_ARRAY(
                                DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH),
                                DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH),
                                INTERVAL 1 MONTH
                            )
                        ) AS month_start_date
                )
        ),
        vars AS (
            SELECT
                DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) AS curr_month
        ),
        fb_dual_sim_groups AS (
            SELECT
                prod.product_name,
                prod.product_cluster,
                prod.package_group_name,
                prod.subscription_plan_id,
                prod.subscription_plan_name,
                prod.group_id,
                cp.scap_instance_id,
                cp.scap_subscription_plan_id,
                cp.scap_subscription_plan_name,
                COUNT(cp.primary_network_name) AS count_imsi
            FROM
                `inm-iar-data-warehouse-dev.commercial_product.commercial_product__monthly_view` cp
                JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_products` prod ON cp.instance_id = prod.instance_id
            WHERE
                DATE_TRUNC(cp.view_month, MONTH) IN (
                    SELECT
                        curr_month
                    FROM
                        vars
                )
                AND cp.status = 'Active'
                AND CONTAINS_SUBSTR(prod.subscription_plan_name, 'FB')
                AND cp.is_closing_base = 1
                AND (
                    prod.group_id IS NOT NULL
                    OR cp.scap_instance_id IS NOT NULL
                )
            GROUP BY
                prod.product_name,
                prod.product_cluster,
                prod.package_group_name,
                prod.subscription_plan_id,
                prod.subscription_plan_name,
                prod.group_id,
                cp.scap_instance_id,
                cp.scap_subscription_plan_id,
                cp.scap_subscription_plan_name
            HAVING
                COUNT(cp.primary_network_name) = 2
        ),
        imsi_list AS (
            SELECT
                prod.product_name,
                prod.product_cluster,
                prod.package_group_name,
                prod.subscription_plan_id,
                prod.subscription_plan_name,
                prod.group_id,
                cp.scap_instance_id,
                cp.scap_subscription_plan_id,
                cp.scap_subscription_plan_name,
                cp.primary_network_name,
                cp.installed_at
            FROM
                `inm-iar-data-warehouse-dev.commercial_product.commercial_product__monthly_view` cp
                JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_customers` cust ON cp.provisioning_account_id = cust.provisioning_account_id
                JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_products` prod ON cp.instance_id = prod.instance_id
                JOIN fb_dual_sim_groups grp ON prod.group_id = grp.group_id
                AND prod.subscription_plan_id = grp.subscription_plan_id
            WHERE
                DATE_TRUNC(cp.view_month, MONTH) = (
                    SELECT
                        curr_month
                    FROM
                        vars
                )
                AND cp.status = 'Active'
                AND CONTAINS_SUBSTR(prod.subscription_plan_name, 'FB')
                AND cp.is_closing_base = 1
            ORDER BY
                grp.group_id
        ),
        data_range1 AS (
            SELECT
                *
            FROM
                `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
            WHERE
                DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                    SELECT
                        month_start_date
                    FROM
                        dates
                )
                AND EXTRACT(
                    DAY
                    FROM
                        gpsrecord_timestamp
                ) BETWEEN 1 AND 10
        ),
        data_range2 AS (
            SELECT
                *
            FROM
                `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
            WHERE
                DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                    SELECT
                        month_start_date
                    FROM
                        dates
                )
                AND EXTRACT(
                    DAY
                    FROM
                        gpsrecord_timestamp
                ) BETWEEN 11 AND 20
        ),
        data_range3 AS (
            SELECT
                *
            FROM
                `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
            WHERE
                DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                    SELECT
                        month_start_date
                    FROM
                        dates
                )
                AND EXTRACT(
                    DAY
                    FROM
                        gpsrecord_timestamp
                ) BETWEEN 21 AND 31
        ),
        position_data_range_1 AS (
            SELECT
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) minus,
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) plus,
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        group_id_t1,
                        DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                    ORDER BY
                        objectid_t1 DESC
                ) AS rk
            FROM
                (
                    SELECT
                        b.group_id AS group_id_t1,
                        a.accessid AS objectid_t1,
                        a.gpsrecord_timestamp AS noted_datetime_t1,
                        a.longitude AS longitude_t1,
                        a.latitude AS latitude_t1
                    FROM
                        data_range1 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t1
                JOIN (
                    SELECT
                        b.group_id AS group_id_t2,
                        a.accessid AS objectid_t2,
                        a.gpsrecord_timestamp AS noted_datetime_t2,
                        a.longitude AS longitude_t2,
                        a.latitude AS latitude_t2
                    FROM
                        data_range1 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t2 ON t1.group_id_t1 = t2.group_id_t2
                AND t1.objectid_t1 <> t2.objectid_t2
            WHERE
                t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
        ),
        position_data_range_2 AS (
            SELECT
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) minus,
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) plus,
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        group_id_t1,
                        DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                    ORDER BY
                        objectid_t1 DESC
                ) AS rk
            FROM
                (
                    SELECT
                        b.group_id group_id_t1,
                        a.accessid objectid_t1,
                        a.gpsrecord_timestamp noted_datetime_t1,
                        a.longitude longitude_t1,
                        a.latitude latitude_t1
                    FROM
                        data_range2 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t1
                JOIN (
                    SELECT
                        b.group_id group_id_t2,
                        a.accessid objectid_t2,
                        a.gpsrecord_timestamp noted_datetime_t2,
                        a.longitude longitude_t2,
                        a.latitude latitude_t2
                    FROM
                        data_range2 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t2 ON t1.group_id_t1 = t2.group_id_t2
                AND t1.objectid_t1 <> t2.objectid_t2
            WHERE
                t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
        ),
        position_data_range_3 AS (
            SELECT
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) minus,
                TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) plus,
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        group_id_t1,
                        DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                    ORDER BY
                        objectid_t1 DESC
                ) AS rk
            FROM
                (
                    SELECT
                        b.group_id group_id_t1,
                        a.accessid objectid_t1,
                        a.gpsrecord_timestamp noted_datetime_t1,
                        a.longitude longitude_t1,
                        a.latitude latitude_t1
                    FROM
                        data_range3 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t1
                JOIN (
                    SELECT
                        b.group_id group_id_t2,
                        a.accessid objectid_t2,
                        a.gpsrecord_timestamp noted_datetime_t2,
                        a.longitude longitude_t2,
                        a.latitude latitude_t2
                    FROM
                        data_range3 a
                        JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.primary_network_name
                    WHERE
                        installed_at IS NOT NULL
                ) t2 ON t1.group_id_t1 = t2.group_id_t2
                AND t1.objectid_t1 <> t2.objectid_t2
            WHERE
                t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
        ),
        combined AS (
            SELECT
                *
            FROM
                position_data_range_1
            UNION ALL
            SELECT
                *
            FROM
                position_data_range_2
            UNION ALL
            SELECT
                *
            FROM
                position_data_range_3
        )
        -- This portion is now handled as part of the published data source
        -- on Tableau Server as we cannot join tables from different projects
        -- in GCP.
        -- ,
        -- vessel AS (
        --     SELECT DISTINCT
        --         install_id AS `l_band_imsi`,
        --         vessel_name AS `l_band_vessel_name`,
        --         vessel_owner AS `l_band_vessel_owner`,
        --         vessel_operator AS `l_band_vessel_operator`,
        --         account_manager
        --     FROM
        --         `inm-bi.maritime_reports.mbu_snapshot_monthly`
        -- )
    SELECT
        ABS(TIMESTAMP_DIFF(minus, plus, HOUR)) AS time_window_in_hour,
        ABS(TIMESTAMP_DIFF(noted_datetime_t1, noted_datetime_t2, MINUTE)) AS between_two_accessid_minutes,
        ST_DISTANCE(ST_GEOGPOINT(longitude_t1, latitude_t1), ST_GEOGPOINT(longitude_t2, latitude_t2)) * 0.00062137 AS distance_miles,
        ST_GEOGPOINT(longitude_t1, latitude_t1) AS GeoPoint1,
        ST_GEOGPOINT(longitude_t2, latitude_t2) AS GeoPoint2,
        *
    FROM
        combined
        -- JOIN vessel ON CAST(combined.objectid_t1 AS STRING) = vessel.l_band_imsi
    WHERE
        rk = 1
    ORDER BY
        3 DESC
);