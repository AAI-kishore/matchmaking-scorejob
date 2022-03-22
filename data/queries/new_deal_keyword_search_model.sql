SELECT 
    hs_object_id AS `Deal ID`,
    type_of_marketer AS `Type of Marketer`,
    industry_experience AS `Industry Experience`,
    industry_experience_other AS `Industry Experience Other`,
    marketer_responsibilities AS `Marketer Responsibilities`,
    tools_tech_experience_needed AS `Tools / Tech Experience Needed`,
    describe_project AS `Describe Project`,
    comments_for_freelancer AS `Comments for Freelancer`,
    match_requirements AS `Match Requirements`
FROM
mh.deal_adaptive_view
WHERE
hs_object_id= @deal_id@