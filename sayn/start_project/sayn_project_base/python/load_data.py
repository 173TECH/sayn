import sqlite3
import logging
from .utils.log_creator import prepare_data, generate_load_query
from sayn import PythonTask


class LoadData(PythonTask):
    def setup(self):
        err = False

        # we use this list to control how many battles we want per tournament
        tournament_battles = [
            {"tournament_id": 1, "n_battles": 1000},
            {"tournament_id": 2, "n_battles": 250},
            {"tournament_id": 3, "n_battles": 500},
        ]

        try:
            self.data_to_load = prepare_data(tournament_battles)
        except Exception as e:
            err = True
            logging.error(e)

        if err:
            return self.failed()
        else:
            return self.ready()

    def run(self):

        # load the logs
        for log_type, log_details in self.data_to_load.items():
            # create table
            logging.info("Creating table: {log_type}.".format(log_type=log_type))

            self.default_db.execute(log_details["create"])

            # load logs
            logging.info("Loading logs: {log_type}.".format(log_type=log_type))
            logs = log_details["data"]

            for log in logs:
                q_insert = generate_load_query(log_type, log)

                self.default_db.execute(q_insert)

            # done
            logging.info("Done: {log_type}.".format(log_type=log_type))

        return self.success()
