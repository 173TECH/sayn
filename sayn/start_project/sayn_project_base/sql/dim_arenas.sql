SELECT JSON_EXTRACT(l.payload, '$.arena_id') arena_id
     , JSON_EXTRACT(l.payload, '$.arena_name') arena_name

FROM logs l

WHERE event_type = 'arenaCreation'
