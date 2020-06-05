SELECT registration_timeline.*
FROM
     (
      SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id", "status_datetime" AS s_dt
        FROM registration_timeline JOIN status_time_table
        WHERE registration_timeline.ingested_datetime < status_time_table.status_datetime
        GROUP BY "nhs_number", "status_datetime"
    )
       INNER JOIN registration_timeline JOIN status_time_table
         ON (((nhs_number = nhs_id) AND (status_datetime = s_dt)) AND (event_order = eo))
)
