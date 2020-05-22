SELECT JSON_EXTRACT(l.payload, '$.fighter_id') fighter_id
     , JSON_EXTRACT(l.payload, '$.fighter_name') fighter_name

FROM logs l

WHERE event_type = 'fighterCreation'
