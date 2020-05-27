import random
import time

# Table creation queries

q_create_fighters = '''
    DROP TABLE IF EXISTS {user_prefix}logs_fighters
    ;

    CREATE TABLE {user_prefix}logs_fighters (
        event_id VARCHAR,
        fighter_id INTEGER,
        fighter_name VARCHAR
    )
    ;
'''

q_create_arenas = '''
    DROP TABLE IF EXISTS {user_prefix}logs_arenas
    ;

    CREATE TABLE {user_prefix}logs_arenas (
        event_id VARCHAR,
        arena_id INTEGER,
        arena_name VARCHAR
    )
    ;
'''

q_create_tournaments = '''
    DROP TABLE IF EXISTS {user_prefix}logs_tournaments
    ;

    CREATE TABLE {user_prefix}logs_tournaments (
        event_id VARCHAR,
        tournament_id INTEGER,
        tournament_name VARCHAR
    )
    ;
'''

q_create_battles = '''
    DROP TABLE IF EXISTS {user_prefix}logs_battles
    ;

    CREATE TABLE {user_prefix}logs_battles (
        event_id VARCHAR,
        tournament_id INTEGER,
        battle_id INTEGER,
        arena_id INTEGER,
        fighter1_id INTEGER,
        fighter2_id INTEGER,
        winner_id INTEGER
    )
    ;
'''

#Log Details

fighter_logs = [
    (1, "son goku"),
    (2, "john"),
    (3, "lucy"),
    (4, "dr. x"),
    (5, "carlos"),
    (6, "el mucho"),
    (7, "maciassito"),
    (8, "kayo"),
    (9, "vatossito"),
    (10, "reniot"),
    (11, "pepe"),
    (12, "anonymous"),
]

arena_logs = [
    (1, "earth canyon"),
    (2, "world edge"),
    (3, "namek"),
    (4, "volcanic crater"),
    (5, "underwater")
]

tournament_logs = [
    (1, "world championships"),
    (2, "tenka-ichi budokai"),
    (3, "king of the mountain")
]

def create_battle_logs(tournament_battles):
    battle_logs = []

    for tb in tournament_battles:
        tournament_id = tb["tournament_id"]
        n_battles = tb["n_battles"]

        for n in range(n_battles):
            battle_id = n + 1
            # determine fighters
            fighter1_id = random.randint(1, 12)
            fighter2_id = random.randint(1, 12)
            # loop to avoid similar fighter
            while fighter1_id == fighter2_id:
                fighter2_id = random.randint(1, 12)
            # determine arenas
            arena_id = random.randint(1, 5)
            # determine winner
            if fighter1_id == 1 or fighter2_id == 1:
                winner_id = 1
            else:
                rand = random.uniform(0, 1)
                if rand <= 0.5:
                    winner_id = fighter1_id
                else:
                    winner_id = fighter2_id

            row = (
                tournament_id,
                battle_id,
                arena_id,
                fighter1_id,
                fighter2_id,
                winner_id
            )

            battle_logs.append(row)

    return battle_logs

# Data Package

def prepare_data(tournament_battles, user_prefix=''):
    logs = {
        'fighters': {
            'create': q_create_fighters.format(user_prefix=user_prefix),
            'data': fighter_logs
        },
        'arenas': {
            'create': q_create_arenas.format(user_prefix=user_prefix),
            'data': arena_logs
        },
        'tournaments': {
            'create': q_create_tournaments.format(user_prefix=user_prefix),
            'data': tournament_logs
        },
        'battles': {
            'create': q_create_battles.format(user_prefix=user_prefix),
            'data': create_battle_logs(tournament_battles)
        },
    }

    return logs

# Log Load

def generate_load_query(log_type, log, user_prefix=''):
    event_id = time.time()

    if log_type == 'fighters':
        q_insert = '''
            INSERT INTO {user_prefix}logs_fighters VALUES('{event_id}', {fighter_id}, '{fighter_name}')
        '''.format(
            user_prefix=user_prefix,
            event_id=event_id,
            fighter_id=log[0],
            fighter_name=log[1]
        )

    elif log_type == 'arenas':
        q_insert = '''
            INSERT INTO {user_prefix}logs_arenas VALUES('{event_id}', {arena_id}, '{arena_name}')
        '''.format(
            user_prefix=user_prefix,
            event_id=event_id,
            arena_id=log[0],
            arena_name=log[1]
        )

    elif log_type == 'tournaments':
        q_insert = '''
            INSERT INTO {user_prefix}logs_tournaments VALUES('{event_id}', {tournament_id}, '{tournament_name}')
        '''.format(
            user_prefix=user_prefix,
            event_id=event_id,
            tournament_id=log[0],
            tournament_name=log[1]
        )

    elif log_type == 'battles':
        q_insert = '''
            INSERT INTO {user_prefix}logs_battles VALUES('{event_id}', {tournament_id}, {battle_id}, {arena_id}, {fighter1_id}, {fighter2_id}, {winner_id})
        '''.format(
            user_prefix=user_prefix,
            event_id=event_id,
            tournament_id=log[0],
            battle_id=log[1],
            arena_id=log[2],
            fighter1_id = log[3],
            fighter2_id = log[4],
            winner_id = log[5]
        )

    else:
        pass

    return q_insert
