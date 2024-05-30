WITH
    vars AS (
        SELECT
            DATE_SUB(
                DATE_TRUNC(CURRENT_DATE(), MONTH),
                INTERVAL 1 MONTH
            ) AS curr_month
    ),
    fb_dual_sim_groups AS (
        SELECT
            prod.product_name, -- dim_product.product_name!
            prod.product_cluster, -- dim_product.product_family!
            prod.package_group_name, -- dim_product.product_description!
            prod.subscription_plan_id, -- dim_package_plan.package_plan_id!
            prod.subscription_plan_name, -- dim_package_plan.package_plan_description
            prod.group_id, -- subscriber_monthly_snapshot.group_id
            cp.scap_instance_id, -- subscriber_monthly_snapshot.group_package_instance_id
            cp.scap_subscription_id, -- rp.rate_plan_id
            cp.scap_subscription_plan_name, -- rate_plan
            COUNT(cp.primary_network_name) AS count_imsi
        FROM
            inm-bi.commercial_product.commercial_product__monthly cp
            JOIN `inm-bi.commercial_product.dim_instance_products` prod ON cp.product_activity_id = prod.product_activity_id
        WHERE
            DATE_TRUNC(cp.view_month, MONTH) = (
                SELECT
                    curr_month
                FROM
                    vars
            )
            AND cp.status = 'Active' -- subscriber_monthly_snapshot.ps_network_id_status = 'Active'
            AND CONTAINS_SUBSTR(prod.subscription_plan_name, 'FB') -- dim_product.product_name = 'FB'
            AND cp.is_closing_base = 1 --  subscriber_monthly_snapshot.closing_base = 1
            -- AND primary_network_name IN ("901112114967151", "901112114967069")
            AND (
                group_id IS NOT NULL
                OR scap_instance_id IS NOT NULL
            )
        GROUP BY
            prod.product_name, -- dim_product.product_name!
            prod.product_cluster, -- dim_product.product_family!
            prod.package_group_name, -- dim_product.product_description!
            prod.subscription_plan_id, -- dim_package_plan.package_plan_id!
            prod.subscription_plan_name, -- dim_package_plan.package_plan_description
            prod.group_id, -- subscriber_monthly_snapshot.group_id
            cp.scap_instance_id, -- subscriber_monthly_snapshot.group_package_instance_id
            cp.scap_subscription_id, -- rp.rate_plan_id
            cp.scap_subscription_plan_name -- rate_plan
        HAVING
            COUNT(cp.primary_network_name) = 2
    ),
    imsi_list AS (
        SELECT
            prod.product_name, -- dim_product.product_name!
            prod.product_cluster, -- dim_product.product_family!
            prod.package_group_name, -- dim_product.product_description!
            prod.subscription_plan_id, -- dim_package_plan.package_plan_id!
            prod.subscription_plan_name, -- dim_package_plan.package_plan_description
            prod.group_id, -- subscriber_monthly_snapshot.group_id
            cp.scap_instance_id, -- subscriber_monthly_snapshot.group_package_instance_id
            cp.scap_subscription_id, -- rp.rate_plan_id
            cp.scap_subscription_plan_name, -- rate_plan
            cp.primary_network_name,
            cp.installed_at
        FROM
            inm-bi.commercial_product.commercial_product__monthly cp
            JOIN inm-bi.commercial_product.dim_instance_customers cust ON cp.provisioning_account_id = cust.provisioning_account_id
            JOIN `inm-bi.commercial_product.dim_instance_products` prod ON cp.product_activity_id = prod.product_activity_id
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
            `network-data-ingest.users_maritime.urs_fb_pos202404`
        WHERE
            EXTRACT(
                DATE
                FROM
                    gpsrecord_timestamp
            ) BETWEEN '2024-04-01' AND '2024-04-10' --and
            --where accessid in (901112114183190,901112114183191)
            --order by accessid,gpsrecord_timestamp desc
    ),
    data_range2 AS (
        SELECT
            *
        FROM
            `network-data-ingest.users_maritime.urs_fb_pos202404`
        WHERE
            EXTRACT(
                DATE
                FROM
                    gpsrecord_timestamp
            ) BETWEEN '2024-04-11' AND '2024-04-20' --and
            --where accessid in (901112114183190,901112114183191)
            --order by accessid,gpsrecord_timestamp desc
    ),
    data_range3 AS (
        SELECT
            *
        FROM
            `network-data-ingest.users_maritime.urs_fb_pos202404`
        WHERE
            EXTRACT(
                DATE
                FROM
                    gpsrecord_timestamp
            ) BETWEEN '2024-04-21' AND '2024-04-30' --and
            --where accessid in (901112114183190,901112114183191)
            --order by accessid,gpsrecord_timestamp desc
    ),
    position_data_range_1 AS (
        SELECT
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL -60 MINUTE) minus,
            TIMESTAMP_ADD(t1.noted_datetime_t1, INTERVAL 60 MINUTE) plus,
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    group_id_t1
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
                    group_id_t1
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
                    group_id_t1
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
SELECT
    --rev.*,us.*,
    ABS(TIMESTAMP_DIFF(minus, plus, HOUR)) time_window_in_hour,
    ABS(
        TIMESTAMP_DIFF(noted_datetime_t1, noted_datetime_t2, MINUTE)
    ) between_two_accessid_minutes,
    ST_DISTANCE(
        ST_GEOGPOINT(longitude_t1, latitude_t1),
        ST_GEOGPOINT(longitude_t2, latitude_t2)
    ) * 0.00062137 distance_miles,
    ST_GEOGPOINT(longitude_t1, latitude_t1) GeoPoint1,
    ST_GEOGPOINT(longitude_t2, latitude_t2) GeoPoint2,
    *
FROM
    combined SUM
    --join usage us on sum.group_id_t1=us.group_id
    --join revenue rev on sum.group_id_t1=rev.group_id
WHERE
    rk = 1 --(rk between 1 and 2)  --and group_id_t1='GRP6264072'
ORDER BY
    3 DESC