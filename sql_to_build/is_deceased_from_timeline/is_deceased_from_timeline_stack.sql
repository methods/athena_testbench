last_deceased_feedback AS (
  SELECT registration_timeline.*
    FROM (
          (
            SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id"
            FROM registration_timeline
            WHERE registration_timeline.event_code='D001'
            GROUP BY "nhs_number"
          ) AS latest_event
           INNER JOIN registration_timeline
                      ON ((nhs_number = nhs_id) AND (event_order = eo))
      )
)
, last_registration AS (
  SELECT registration_timeline.*
    FROM (
          (
            SELECT "max"("event_order") "eo", "nhs_number" AS "nhs_id"
            FROM registration_timeline
            WHERE registration_timeline.has_access_to_essential_supplies = 'NO'
            GROUP BY "nhs_number"
          ) AS latest_event
           INNER JOIN registration_timeline
                      ON ((nhs_number = nhs_id) AND (event_order = eo))
      )
)
, is_deceased_from_timeline AS (
  SELECT last_deceased_feedback.nhs_number AS nhs_number,
        '1' AS is_deceased
  FROM last_deceased_feedback
  LEFT JOIN last_registration on last_deceased_feedback.nhs_number = last_registration.nhs_number
  WHERE (last_registration.event_order IS null) OR (last_registration.event_order <= last_deceased_feedback.event_order)
)
