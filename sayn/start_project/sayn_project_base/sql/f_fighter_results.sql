WITH fighter1_outcome AS (

  SELECT b.tournament_name
       , b.battle_id
       , b.arena_name
       , b.fighter1_name
       , CASE WHEN b.fighter1_name = b.winner_name THEN 1 ELSE 0 END is_winner

  FROM f_battles b
)

, fighter2_outcome AS (

  SELECT b.tournament_name
       , b.battle_id
       , b.arena_name
       , b.fighter2_name
       , CASE WHEN b.fighter2_name = b.winner_name THEN 1 ELSE 0 END is_winner

  FROM f_battles b
)

SELECT f1.tournament_name
     , f1.battle_id
     , f1.arena_name
     , f1.fighter1_name fighter_name
     , f1.is_winner

FROM fighter1_outcome f1

UNION

SELECT f2.tournament_name
     , f2.battle_id
     , f2.arena_name
     , f2.fighter2_name fighter_name
     , f2.is_winner

FROM fighter2_outcome f2
