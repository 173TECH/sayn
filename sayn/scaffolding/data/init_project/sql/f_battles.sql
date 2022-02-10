SELECT t.tournament_name
     , t.tournament_name || '-' || CAST(b.battle_id AS VARCHAR) AS battle_id
     , a.arena_name
     , f1.fighter_name AS fighter1_name
     , f2.fighter_name AS fighter2_name
     , w.fighter_name AS winner_name

  FROM logs_battles b

  LEFT JOIN {{ src('dim_tournaments') }} t
    ON b.tournament_id = t.tournament_id

  LEFT JOIN {{ src('dim_arenas') }} a
    ON b.arena_id = a.arena_id

  LEFT JOIN {{ src('dim_fighters') }} f1
    ON b.fighter1_id = f1.fighter_id

  LEFT JOIN {{ src('dim_fighters') }} f2
    ON b.fighter2_id = f2.fighter_id

  LEFT JOIN {{ src('dim_fighters') }} w
    ON b.winner_id = w.fighter_id
