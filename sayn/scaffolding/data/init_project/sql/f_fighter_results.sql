SELECT b.tournament_name
     , b.battle_id
     , b.arena_name
     , b.fighter1_name AS fighter_name
     , CASE WHEN b.fighter1_name = b.winner_name THEN 1 ELSE 0 END AS is_winner
  FROM {{ src('f_battles') }} b

 UNION

SELECT b.tournament_name
     , b.battle_id
     , b.arena_name
     , b.fighter2_name AS fighter_name
     , CASE WHEN b.fighter2_name = b.winner_name THEN 1 ELSE 0 END AS is_winner
  FROM {{ src('f_battles') }} b
