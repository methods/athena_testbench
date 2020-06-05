WITH all_submissions AS (@all_submissions.sql@)
   , all_la_feedback AS (@all_la_feedback.sql@)
   , wholesaler_opt_out_list AS (@wholesaler_opt_out_list.sql@)
   , registration_timeline AS (@registration_timeline_select.sql@)
   , registration_status AS (@registration_status_select.sql@)

select * from registration_status