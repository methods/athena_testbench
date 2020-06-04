WITH all_submissions AS (@all_submissions.sql@)

, latest_submission AS (@latest_submission.sql@)


/* LA FEEDBACK */


, all_la_feedback AS (@all_la_feedback.sql@)


/* LA FEEDBACK - CONTINUE */


, feedback_list_filtered_continue AS (@feedback_list_filtered_continue.sql@)

, feedback_list_continue AS (@feedback_list_continue.sql@)

, feedback_list_deduplicated_continue AS (@feedback_list_deduplicated_continue.sql@)

, latest_la_feedback_to_continue_boxes AS (@latest_la_feedback_to_continue_boxes.sql@)


/* LA FEEDBACK - OPT OUT */


, feedback_list_filtered_opt_out AS (@feedback_list_filtered_opt_out.sql@)

, feedback_list_opt_out AS (@feedback_list_opt_out.sql@)

, feedback_list_deduplicated_opt_out AS (@feedback_list_deduplicated_opt_out.sql@)

, latest_la_feedback_to_stop_boxes AS (@latest_la_feedback_to_stop_boxes.sql@)


/* WHOLESALER FEEDBACK */

, wholesaler_opt_out_list AS (@wholesaler_opt_out_list.sql@)

, wholesaler_opt_out_list_deduplicated AS (@wholesaler_opt_out_list_deduplicated.sql@)

, latest_wholesaler_opt_out AS (@latest_wholesaler_opt_out.sql@)

/* STAGING OUTPUT */


, full_submission_record AS (@full_submission_record.sql@)


/* FINAL OUTPUT */
, current_output AS (@current_wholesaler_latest_user_submission_select.sql@)
, new_output AS (@wholesaler_latest_user_submission_select.sql@)

, current_no_list AS (
  select * from current_output where EssentialSupplies='NO'
)
, new_no_list AS (
  select * from current_output where EssentialSupplies='NO'
)

select current_no_list.* from current_no_list
  left join new_no_list on current_no_list.ID = new_no_list.ID
where new_no_list.ID is null