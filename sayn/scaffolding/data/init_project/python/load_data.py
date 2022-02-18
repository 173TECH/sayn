import random
from uuid import uuid4

from sayn import task


@task(outputs=["logs_arenas", "logs_tournaments", "logs_battles", "logs_fighters"])
def load_data(context, warehouse):
    fighters = ["Son Goku", "John", "Lucy", "Dr. x", "Carlos", "Dr. y?"]
    arenas = ["Earth Canyon", "World Edge", "Namek", "Volcanic Crater", "Underwater"]
    tournaments = ["World Championships", "Tenka-ichi Budokai", "King of the Mountain"]

    context.set_run_steps(
        [
            "Generate Dimensions",
            "Generate Battles",
            "Load fighters",
            "Load arenas",
            "Load tournaments",
            "Load battles",
        ]
    )

    with context.step("Generate Dimensions"):
        # Add ids to the dimensions
        fighters = [
            {"fighter_id": str(uuid4()), "fighter_name": val}
            for id, val in enumerate(fighters)
        ]
        arenas = [
            {"arena_id": str(uuid4()), "arena_name": val}
            for id, val in enumerate(arenas)
        ]
        tournaments = [
            {"tournament_id": str(uuid4()), "tournament_name": val}
            for id, val in enumerate(tournaments)
        ]

    with context.step("Generate Battles"):
        battles = list()
        for tournament in tournaments:
            tournament_id = tournament["tournament_id"]
            # Randomly select a number of battles to generate for each tournament
            n_battles = random.choice([10, 20, 30])

            for _ in range(n_battles):
                battle_id = str(uuid4())

                # Randomly choose fighters and arena
                fighter1_id = random.choice(fighters)["fighter_id"]
                fighter2_id = random.choice(
                    [f for f in fighters if f["fighter_id"] != fighter1_id]
                )["fighter_id"]
                arena_id = random.choice(arenas)["arena_id"]

                # Pick a winner
                winner_id = fighter1_id if random.uniform(0, 1) <= 0.5 else fighter2_id

                battles.append(
                    {
                        "event_id": str(uuid4()),
                        "tournament_id": tournament_id,
                        "battle_id": battle_id,
                        "arena_id": arena_id,
                        "fighter1_id": fighter1_id,
                        "fighter2_id": fighter2_id,
                        "winner_id": winner_id,
                    }
                )

    data_to_load = {
        "fighters": fighters,
        "arenas": arenas,
        "tournaments": tournaments,
        "battles": battles,
    }

    # Load logs
    for log_type, log_data in data_to_load.items():
        with context.step(f"Load {log_type}"):
            warehouse.load_data(f"logs_{log_type}", log_data, replace=True)
