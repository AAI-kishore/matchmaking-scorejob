SELECT 
    hs_object_id AS `Deal ID`,
    deal_priority AS `Deal Priority`,
    level_of_engagement AS `What level of engagement will you require from the marketer?`,
    type_of_marketer AS `Type of Marketer`,
    industry_experience AS `Industry Experience`,
    how_soon_do_you_need_the_marketer_ AS `How soon do you need the marketer?`,
    weekly_or_monthly_budget AS `Weekly or Hourly Budget`,
    company_size AS `How big is your company?`,
    marketer_duration AS `How long do you need the marketer?`
FROM
mh.deal_adaptive_view
WHERE
hs_object_id= @deal_id@