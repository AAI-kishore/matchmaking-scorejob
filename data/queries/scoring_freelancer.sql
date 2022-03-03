SELECT *
FROM
sb_ai.preprocessed_freelancer
WHERE (status='Approved') and (role like '%%variable_role%%');
