status_before AS (
    SELECT registration_timeline.*
    FROM (
          (
            SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id"
            FROM registration_timeline
            WHERE registration_timeline.ingested_datetime < date_parse('${from_timestamp}', '%Y-%m-%d %H:%i:%s')
            GROUP BY "nhs_number"
          ) AS latest_event
           INNER JOIN registration_timeline
                      ON ((nhs_number = nhs_id) AND (event_order = eo))
      )
  )
, status_after AS (
  SELECT registration_timeline.*
  FROM (
        (
          SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id"
          FROM registration_timeline
          WHERE registration_timeline.ingested_datetime <= date_parse('${to_timestamp}', '%Y-%m-%d %H:%i:%s')
          GROUP BY "nhs_number"
        ) AS latest_event
         INNER JOIN registration_timeline
                    ON ((nhs_number = nhs_id) AND (event_order = eo))
    )
)
, change_between_dates AS (
  SELECT status_after.*
  FROM status_after
         LEFT JOIN status_before on status_after.nhs_number = status_before.nhs_number
  WHERE NOT status_after.has_access_to_essential_supplies = status_before.has_access_to_essential_supplies
)
