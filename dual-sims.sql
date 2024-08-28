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
            UNNEST(
                GENERATE_DATE_ARRAY(
                    DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 6 MONTH),
                    DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH),
                    INTERVAL 1 MONTH
                )
            ) AS month_start_date
    ),

    vars AS (
        SELECT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) AS curr_month
    ),

    fb_dual_sim_groups AS (
        SELECT
            prd.product_name,
            prd.product_cluster,
            prd.package_group_name,
            prd.subscription_plan_id,
            prd.subscription_plan_name,
            prd.group_id,
            cp.scap_instance_id,
            cp.scap_subscription_plan_id,
            cp.scap_subscription_plan_name,
            COUNT(cp.primary_network_name) AS count_imsi
        FROM
            `inm-iar-data-warehouse-dev.commercial_product.commercial_product__monthly_view` AS cp
        INNER JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_products` AS prd
            ON cp.instance_id = prd.instance_id
        WHERE
            DATE_TRUNC(cp.view_month, MONTH) IN (
                SELECT curr_month FROM vars
            )
            AND cp.status = 'Active'
            AND CONTAINS_SUBSTR(prd.subscription_plan_name, 'FB')
            AND cp.is_closing_base = 1
            AND (prd.group_id IS NOT NULL OR cp.scap_instance_id IS NOT NULL)
        GROUP BY
            prd.product_name,
            prd.product_cluster,
            prd.package_group_name,
            prd.subscription_plan_id,
            prd.subscription_plan_name,
            prd.group_id,
            cp.scap_instance_id,
            cp.scap_subscription_plan_id,
            cp.scap_subscription_plan_name
        HAVING
            COUNT(cp.primary_network_name) = 2
    ),

    imsi_list AS (
        SELECT
            prd.product_name,
            prd.product_cluster,
            prd.package_group_name,
            prd.subscription_plan_id,
            prd.subscription_plan_name,
            prd.group_id,
            cp.scap_instance_id,
            cp.scap_subscription_plan_id,
            cp.scap_subscription_plan_name,
            cp.primary_network_name,
            cp.installed_at
        FROM
            `inm-iar-data-warehouse-dev.commercial_product.commercial_product__monthly_view` AS cp
        INNER JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_customers` AS cust
            ON cp.provisioning_account_id = cust.provisioning_account_id
        INNER JOIN `inm-iar-data-warehouse-dev.commercial_product.dim_instance_products` AS prd
            ON cp.instance_id = prd.instance_id
        INNER JOIN fb_dual_sim_groups AS grp
            ON prd.group_id = grp.group_id
            AND prd.subscription_plan_id = grp.subscription_plan_id
        WHERE
            DATE_TRUNC(cp.view_month, MONTH) = (
                SELECT curr_month FROM vars
            )
            AND cp.status = 'Active'
            AND CONTAINS_SUBSTR(prd.subscription_plan_name, 'FB')
            AND cp.is_closing_base = 1
        ORDER BY
            grp.group_id
    ),

    data_range_1 AS (
        SELECT
            *
        FROM
            `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
        WHERE
            DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                SELECT month_start_date FROM dates
            )
            AND EXTRACT(DAY FROM gpsrecord_timestamp) BETWEEN 1 AND 10
    ),

    data_range_2 AS (
        SELECT
            *
        FROM
            `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
        WHERE
            DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                SELECT month_start_date FROM dates
            )
            AND EXTRACT(DAY FROM gpsrecord_timestamp) BETWEEN 11 AND 20
    ),

    data_range_3 AS (
        SELECT
            *
        FROM
            `network-data-ingest.users_maritime.urs_fb_pos_prev_6mths_tab`
        WHERE
            DATE(DATE_TRUNC(gpsrecord_timestamp, MONTH)) IN (
                SELECT month_start_date FROM dates
            )
            AND EXTRACT(DAY FROM gpsrecord_timestamp) BETWEEN 21 AND 31
    ),

    position_data_range_1 AS (
        SELECT
            t1.*,
            t2.*,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AS minus,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) AS plus,
            ROW_NUMBER() OVER (
                PARTITION BY
                    t1.group_id_t1,
                    DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                ORDER BY
                    t1.objectid_t1 DESC
            ) AS group_id_rank
        FROM
            (
                SELECT
                    i.group_id AS group_id_t1,
                    dr1.accessid AS objectid_t1,
                    dr1.gpsrecord_timestamp AS noted_datetime_t1,
                    dr1.longitude AS longitude_t1,
                    dr1.latitude AS latitude_t1
                FROM
                    data_range_1 AS dr1
                INNER JOIN imsi_list AS i ON CAST(dr1.accessid AS STRING) = i.primary_network_name
                WHERE
                    i.installed_at IS NOT NULL
            ) AS t1
        INNER JOIN (
            SELECT
                i.group_id AS group_id_t2,
                dr1.accessid AS objectid_t2,
                dr1.gpsrecord_timestamp AS noted_datetime_t2,
                dr1.longitude AS longitude_t2,
                dr1.latitude AS latitude_t2
            FROM
                data_range_1 AS dr1
            INNER JOIN imsi_list AS i ON CAST(dr1.accessid AS STRING) = i.primary_network_name
            WHERE
                i.installed_at IS NOT NULL
        ) AS t2
            ON t1.group_id_t1 = t2.group_id_t2
            AND t1.objectid_t1 != t2.objectid_t2
        WHERE
            t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(
                t1.noted_datetime_t1, INTERVAL -60 MINUTE
            ) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
    ),

    position_data_range_2 AS (
        SELECT
            t1.*,
            t2.*,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AS minus,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) AS plus,
            ROW_NUMBER() OVER (
                PARTITION BY
                    t1.group_id_t1,
                    DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                ORDER BY
                    t1.objectid_t1 DESC
            ) AS group_id_rank
        FROM
            (
                SELECT
                    i.group_id AS group_id_t1,
                    dr2.accessid AS objectid_t1,
                    dr2.gpsrecord_timestamp AS noted_datetime_t1,
                    dr2.longitude AS longitude_t1,
                    dr2.latitude AS latitude_t1
                FROM
                    data_range_2 AS dr2
                INNER JOIN imsi_list AS i ON CAST(dr2.accessid AS STRING) = i.primary_network_name
                WHERE
                    i.installed_at IS NOT NULL
            ) AS t1
        INNER JOIN (
            SELECT
                i.group_id AS group_id_t2,
                dr2.accessid AS objectid_t2,
                dr2.gpsrecord_timestamp AS noted_datetime_t2,
                dr2.longitude AS longitude_t2,
                dr2.latitude AS latitude_t2
            FROM
                data_range_2 AS dr2
            INNER JOIN imsi_list AS i ON CAST(dr2.accessid AS STRING) = i.primary_network_name
            WHERE
                i.installed_at IS NOT NULL
        ) AS t2
            ON t1.group_id_t1 = t2.group_id_t2
            AND t1.objectid_t1 != t2.objectid_t2
        WHERE
            t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(
                t1.noted_datetime_t1, INTERVAL -60 MINUTE
            ) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
    ),

    position_data_range_3 AS (
        SELECT
            t1.*,
            t2.*,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) AS minus,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) AS plus,
            ROW_NUMBER() OVER (
                PARTITION BY
                    t1.group_id_t1,
                    DATE_TRUNC(t1.noted_datetime_t1, MONTH)
                ORDER BY
                    t1.objectid_t1 DESC
            ) AS group_id_rank
        FROM
            (
                SELECT
                    i.group_id AS group_id_t1,
                    dr3.accessid AS objectid_t1,
                    dr3.gpsrecord_timestamp AS noted_datetime_t1,
                    dr3.longitude AS longitude_t1,
                    dr3.latitude AS latitude_t1
                FROM
                    data_range_3 AS dr3
                INNER JOIN imsi_list AS i ON CAST(dr3.accessid AS STRING) = i.primary_network_name
                WHERE
                    i.installed_at IS NOT NULL
            ) AS t1
        INNER JOIN (
            SELECT
                i.group_id AS group_id_t2,
                dr3.accessid AS objectid_t2,
                dr3.gpsrecord_timestamp AS noted_datetime_t2,
                dr3.longitude AS longitude_t2,
                dr3.latitude AS latitude_t2
            FROM
                data_range_3 AS dr3
            INNER JOIN imsi_list AS i ON CAST(dr3.accessid AS STRING) = i.primary_network_name
            WHERE
                i.installed_at IS NOT NULL
        ) AS t2
            ON t1.group_id_t1 = t2.group_id_t2
            AND t1.objectid_t1 != t2.objectid_t2
        WHERE
            t2.noted_datetime_t2 BETWEEN TIMESTAMP_ADD(
                t1.noted_datetime_t1, INTERVAL -60 MINUTE
            ) AND TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE)
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

    SELECT
        ABS(TIMESTAMP_DIFF(minus, plus, HOUR)) AS time_window_in_hour,
        ABS(TIMESTAMP_DIFF(noted_datetime_t1, noted_datetime_t2, MINUTE))
            AS between_two_accessid_minutes,
        ST_DISTANCE(
            ST_GEOGPOINT(longitude_t1, latitude_t1), ST_GEOGPOINT(longitude_t2, latitude_t2)
        )
        * 0.00062137 AS distance_miles,
        ST_GEOGPOINT(longitude_t1, latitude_t1) AS geopoint1,
        ST_GEOGPOINT(longitude_t2, latitude_t2) AS geopoint2,
        *
    FROM
        combined
    WHERE
        group_id_rank = 1
);
