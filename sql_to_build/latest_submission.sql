SELECT
  provenance,
  nhs_number,
  submission_time,
  has_access_to_essential_supplies,
  is_able_to_carry_supplies,
  email_address,
  phone_number_calls,
  phone_number_texts
FROM (
  SELECT
    MAX(submission_time) AS st,
    nhs_number AS nn
  FROM all_submissions
  GROUP BY nhs_number
)
JOIN all_submissions
  ON nn = nhs_number
  AND st = submission_time
