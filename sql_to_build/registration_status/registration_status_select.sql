SELECT registration_timeline.*
FROM (
      (
        SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id"
        FROM registration_timeline
        WHERE registration_timeline.ingested_datetime < date_parse('${timestamp}', '%Y-%m-%d %H:%i:%s')
        GROUP BY "nhs_number"
      ) AS latest_event
       INNER JOIN registration_timeline
                  ON ((nhs_number = nhs_id) AND (event_order = eo))
  )
