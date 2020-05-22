SELECT JSON_EXTRACT(l.payload, '$.tournament_id') tournament_id
     , JSON_EXTRACT(l.payload, '$.tournament_name') tournament_name

FROM logs l

WHERE event_type = 'tournamentCreation'
