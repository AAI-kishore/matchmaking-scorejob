SELECT *
FROM
mh.preprocessed_freelancer
WHERE (`Status`='Approved') and (`Role` like '%%variable_role%%');
