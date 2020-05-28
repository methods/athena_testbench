SELECT
    CAST("nhsnumber" AS varchar) "nhs_number"
    , "inputoutcomecode" "feedback_code"
    , COALESCE(
        TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s')),
        TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s.%f')),
        TRY("date_parse"("inputcompletedoutcomedate", '%d-%m-%Y')),
        TRY("date_parse"("inputcompletedoutcomedate", '%d/%m/%Y'))
        ) "feedback_time"
    , "inputoutcomecomments" "feedback_comments"
FROM
    raw_la_outcomes_staging
