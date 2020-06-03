SELECT
    CAST("nhsnumber" AS varchar) AS "nhs_number"
    , "inputoutcomecode" AS "feedback_code"
    , COALESCE(
        TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s')),
        TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s.%f')),
        TRY("date_parse"("inputcompletedoutcomedate", '%d-%m-%Y')),
        TRY("date_parse"("inputcompletedoutcomedate", '%d/%m/%Y'))
        ) AS "feedback_time"
    , "inputoutcomecomments" AS "feedback_comments"
FROM
    raw_la_outcomes_staging
