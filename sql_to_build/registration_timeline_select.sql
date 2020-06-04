SELECT nhs_nhs_number AS nhs_number, ID, provenance, has_access_to_essential_supplies, event_datetime, ingested_datetime
FROM (
       (
         SELECT LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', nhs_number))))) AS ID,
                provenance                                                                    AS provenance,
                has_access_to_essential_supplies                                              AS has_access_to_essential_supplies,
                submission_time                                                               AS event_datetime,
                submission_time                                                               AS ingested_datetime
         FROM all_submissions
       )
       UNION ALL
       (
         SELECT LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', nhs_number))))) AS ID,
                'local authority'                                                             AS provenance,
                'YES'                                                                         AS has_access_to_essential_supplies,
                feedback_time                                                                 AS event_datetime,
                ingested_datetime                                                             AS ingested_datetime
         FROM all_la_feedback
         where feedback_code IN ('W002', 'W003', 'F002', 'D001')
       )
       UNION ALL
       (
         SELECT wholesaler_id            AS ID,
                'wholesaler'             AS provenance,
                'YES'                    AS has_access_to_essential_supplies,
                wholesaler_delivery_date AS event_datetime,
                ingested_datetime        AS ingested_datetime
         FROM wholesaler_opt_out_list
       )
     ) AS all_events
       JOIN nhs_clean_staging
            ON LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', nhs_nhs_number))))) = ID
ORDER BY nhs_nhs_number, event_datetime, ingested_datetime