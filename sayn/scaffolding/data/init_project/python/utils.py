import random
from uuid import uuid4

# Table creation queries

log_types = {
    "fighters": {
        "columns": {"fighter_id": "INTEGER", "fighter_name": "VARCHAR"},
        "values": [
            "Son Goku",
            "John",
            "Lucy",
            "Dr. x",
            "Carlos",
            "El mucho",
            "Dr. y?",
            "Kayo",
            "Vatossito",
            "Reniot",
            "Reniota",
            "Anonymous",
        ],
    },
    "arenas": {
        "columns": {"arena_id": "INTEGER", "arena_name": "VARCHAR"},
        "values": [
            "Earth Canyon",
            "World Edge",
            "Namek",
            "Volcanic Crater",
            "Underwater",
        ],
    },
    "tournaments": {
        "columns": {"tournament_id": "INTEGER", "tournament_name": "VARCHAR"},
        "values": [
            "World Championships",
            "Tenka-ichi Budokai",
            "King of the Mountain",
        ],
    },
    "battles": {
        "columns": {
            "event_id": "VARCHAR",
            "tournament_id": "INTEGER",
            "battle_id": "INTEGER",
            "arena_id": "INTEGER",
            "fighter1_id": "INTEGER",
            "fighter2_id": "INTEGER",
            "winner_id": "INTEGER",
        }
    },
}


def create_battle_logs(tournament_battles):
    battle_logs = []

    for tb in tournament_battles:
        tournament_id = tb["tournament_id"]
        n_battles = tb["n_battles"]

        for battle_id in range(n_battles):
            # determine fighters
            fighter1_id = random.randrange(len(log_types["fighters"]["values"]))
            fighter2_id = random.randrange(len(log_types["fighters"]["values"]))
            # loop to avoid same fighter
            while fighter1_id == fighter2_id:
                fighter2_id = random.randrange(len(log_types["fighters"]["values"]))

            # determine arenas
            arena_id = random.randrange(len(log_types["arenas"]["values"]))
            # determine winner
            if fighter1_id == 0 or fighter2_id == 0:
                winner_id = 0
            else:
                rand = random.uniform(0, 1)
                if rand <= 0.5:
                    winner_id = fighter1_id
                else:
                    winner_id = fighter2_id

            row = {
                "event_id": str(uuid4()),
                "tournament_id": tournament_id,
                "battle_id": battle_id,
                "arena_id": arena_id,
                "fighter1_id": fighter1_id,
                "fighter2_id": fighter2_id,
                "winner_id": winner_id,
            }

            battle_logs.append(row)

    return battle_logs


# Data Package


def get_data(tournament_battles, user_prefix=""):
    logs = {
        "fighters": [
            {"fighter_id": id, "fighter_name": val}
            for id, val in enumerate(log_types["fighters"]["values"])
        ],
        "arenas": [
            {"arena_id": id, "arena_name": val}
            for id, val in enumerate(log_types["arenas"]["values"])
        ],
        "tournaments": [
            {"tournament_id": id, "tournament_name": val}
            for id, val in enumerate(log_types["tournaments"]["values"])
        ],
        "battles": create_battle_logs(tournament_battles),
    }

    return logs


# Log Load


def get_create_table(log_type, user_prefix=""):
    q_drop = f"DROP TABLE IF EXISTS {user_prefix}logs_{log_type}"

    columns_def = ",\n".join(
        [f"{column} {type}" for column, type in log_types[log_type]["columns"].items()]
    )
    q_create = f"CREATE TABLE {user_prefix}logs_{log_type} ({columns_def})"

    return f"{q_drop};\n\n{q_create};"
