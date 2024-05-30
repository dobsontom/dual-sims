WITH
    vars AS (
        SELECT
            20240430 AS curr_month --*review month please change it as per the review month
            /*,
            TIMESTAMP("2020-12-01 00:00:00") month_start_date,
            TIMESTAMP("2020-12-31 23:59:59") month_end_date,
            TIMESTAMP("2020-10-01 00:00:00") p1_start_date,
            TIMESTAMP("2020-10-31 23:59:59") p1_end_date,
            TIMESTAMP("2020-11-01 00:00:00") p2_start_date,
            TIMESTAMP("2020-11-30 23:59:59") p2_end_date,
            TIMESTAMP("2020-12-01 00:00:00") p3_start_date,
            TIMESTAMP("2020-12-31 23:59:59") p3_end_date
             */
    ),
    FB_DUAL_SIM_GROUPS AS (
        SELECT
            prod.product_name,
            prod.product_family,
            prod.product_description,
            pp.package_plan_id,
            pp.package_plan_description,
            sms.group_package_instance_id,
            sms.group_id,
            rp.rate_plan_id,
            rate_plan,
            COUNT(current_network_id) count_imsi
        FROM
            `inmarsat-datalake-prod.provisioning_subscribers.subscriber_monthly_snapshot` sms
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_rate_plan` rp ON sms.curr_rate_plan_key = rp.rate_plan_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_dp` dp ON sms.dp_key = dp.dp_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_package_plan` pp ON sms.curr_package_plan_key = pp.package_plan_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_product` prod ON sms.product_key = prod.product_key
        WHERE
            snapshot_month_key = (
                SELECT
                    curr_month
                FROM
                    vars
            ) --20230131
            --and group_package_instance_id ='GRP6278322'
            --and sms.current_network_id in ('901112114008766','901112114008767')
            AND ps_network_id_status = 'Active'
            AND product_name = 'FB'
            AND closing_base = 1
            AND (
                sms.group_id <> 'NA'
                OR sms.group_package_instance_id <> 'NA'
            )
            --and sms.group_id='GROUP-1764'
        GROUP BY
            prod.product_name,
            prod.product_family,
            prod.product_description,
            pp.package_plan_id,
            pp.package_plan_description,
            sms.group_package_instance_id,
            sms.group_id,
            rp.rate_plan_id,
            rate_plan
            --and current_network_id='901112114183191'
        HAVING
            COUNT(sms.current_network_id) = 2
    ),
    imsi_list AS (
        SELECT
            prod.product_name,
            prod.product_family,
            prod.product_description,
            pp.package_plan_id,
            pp.package_plan_description,
            dp.customer_node_id,
            dp.dp_id,
            dp.node_name,
            dp.schedule_id,
            dp.billing_profile_id,
            rp.rate_plan_id,
            rp.rate_plan,
            sms.*
        FROM
            `inmarsat-datalake-prod.provisioning_subscribers.subscriber_monthly_snapshot` sms
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_rate_plan` rp ON sms.curr_rate_plan_key = rp.rate_plan_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_dp` dp ON sms.dp_key = dp.dp_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_package_plan` pp ON sms.curr_package_plan_key = pp.package_plan_key
            JOIN `inmarsat-datalake-prod.provisioning_subscribers.dim_product` prod ON sms.product_key = prod.product_key
            JOIN FB_DUAL_SIM_GROUPS grp ON sms.group_id = grp.group_id
            AND prod.product_description = grp.product_description
        WHERE
            snapshot_month_key = (
                SELECT
                    curr_month
                FROM
                    vars
            ) --20230131
            --and sms.group_package_instance_id ='GRP6278322'
            --and sms.group_id ='SSC_2876'
            AND ps_network_id_status = 'Active'
            AND prod.product_name = 'FB'
            AND closing_base = 1
        ORDER BY
            grp.group_id
            --and current_network_id='901112114183191'
    ),
    --select * from imsi_list
    --*review month please change it as per the review month i.e. urs_fb_pos202207 this needs to be changed  
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
                    b.group_id group_id_t1,
                    a.accessid objectid_t1,
                    a.gpsrecord_timestamp noted_datetime_t1,
                    a.longitude longitude_t1,
                    a.latitude latitude_t1
                FROM
                    data_range1 a
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
            ) t1
            JOIN (
                SELECT
                    b.group_id group_id_t2,
                    a.accessid objectid_t2,
                    a.gpsrecord_timestamp noted_datetime_t2,
                    a.longitude longitude_t2,
                    a.latitude latitude_t2
                FROM
                    data_range1 a
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
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
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
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
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
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
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
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
                    JOIN imsi_list b ON CAST(a.accessid AS STRING) = b.current_network_id
                WHERE
                    Activation_Date IS NOT NULL
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
    combined summ
    --join usage us on summ.group_id_t1=us.group_id
    --join revenue rev on summ.group_id_t1=rev.group_id
WHERE
    rk = 1 --(rk between 1 and 2)  --and group_id_t1='GRP6264072'
ORDER BY
    3 DESC