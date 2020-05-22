import json
import sqlite3
import random
import logging
from sayn import PythonTask

#initiate log counter
lcount = 1

class LoadData(PythonTask):
    def setup(self):
       #code doing setup
       err = False
       if err:
           return self.failed()
       else:
           return self.ready()

    def run(self):

        #log loader
        def load_logs(logs, event_type):
            global lcount

            for l in logs:
                q_insert = '''
                    INSERT INTO logs VALUES({lcount}, "{event_type}", '{payload}')
                '''.format(
                    lcount=lcount,
                    event_type=event_type,
                    payload=json.dumps(l)
                )

                self.default_db.execute(q_insert)

                lcount += 1

        #create logs table
        logging.debug("Creating log table.")

        q_create = '''
            DROP TABLE IF EXISTS logs
            ;

            CREATE TABLE logs (
                event_id INTEGER PRIMARY_KEY,
                event_type VARCHAR,
                payload JSON
            )
            ;
            '''

        self.default_db.execute(q_create)

        #create contestants logs
        contestants = [
            {"fighter_id": 1, "fighter_name": "son goku"},
            {"fighter_id": 2, "fighter_name": "john"},
            {"fighter_id": 3, "fighter_name": "lucy"},
            {"fighter_id": 4, "fighter_name": "dr. x"},
            {"fighter_id": 5, "fighter_name": "carlos"},
            {"fighter_id": 6, "fighter_name": "el mucho"},
            {"fighter_id": 7, "fighter_name": "maciassito"},
            {"fighter_id": 8, "fighter_name": "kayo"},
            {"fighter_id": 9, "fighter_name": "vatossito"},
            {"fighter_id": 10, "fighter_name": "renito"},
            {"fighter_id": 11, "fighter_name": "pepe"},
            {"fighter_id": 12, "fighter_name": "anonymous"},
        ]

        logging.debug("Logging contestants.")
        load_logs(contestants, "fighterCreation")

        #create arenas logs
        arenas = [
            {"arena_id": 1, "arena_name": "earth canyon"},
            {"arena_id": 2, "arena_name": "world edge"},
            {"arena_id": 3, "arena_name": "namek"},
            {"arena_id": 4, "arena_name": "volcanic crater"},
            {"arena_id": 5, "arena_name": "underwater"},
        ]

        logging.debug("Logging arenas.")
        load_logs(arenas, "arenaCreation")

        #create tournaments logs
        tournaments = [
            {"tournament_id": 1, "tournament_name": "world championships"},
            {"tournament_id": 2, "tournament_name": "tenka-ichi budokai"},
            {"tournament_id": 3, "tournament_name": "king of the mountain"},
        ]

        logging.debug("Logging tournaments.")
        load_logs(tournaments, "tournamentCreation")

        #create battle logs
        battles = []

        tournament_battles = [
            {"tournament_id": 1, "n_battles": 1000},
            {"tournament_id": 2, "n_battles": 250},
            {"tournament_id": 3, "n_battles": 500},
        ]

        for tb in tournament_battles:
            tournament_id = tb["tournament_id"]
            n_battles = tb["n_battles"]

            for n in range(n_battles):
                battle_id = n + 1
                #determine fighters
                fighter1_id = random.randint(1, 12)
                fighter2_id = random.randint(1, 12)
                #loop to avoid similar fighter
                while fighter1_id == fighter2_id:
                    fighter2_id = random.randint(1, 12)
                #determine arenas
                arena_id = random.randint(1, 5)
                #determine winner
                if fighter1_id == 1 or fighter2_id == 1:
                    winner_id = 1
                else:
                    rand = random.uniform(0, 1)
                    if rand <= 0.5:
                        winner_id = fighter1_id
                    else:
                        winner_id = fighter2_id

                payload = {
                    "tournament_id": tournament_id,
                    "battle_id": battle_id,
                    "arena_id": arena_id,
                    "fighter1_id": fighter1_id,
                    "fighter2_id": fighter2_id,
                    "winner_id": winner_id
                }

                battles.append(payload)

        logging.debug("Logging battles.")
        load_logs(battles, "battleCreation")

        return self.success()
