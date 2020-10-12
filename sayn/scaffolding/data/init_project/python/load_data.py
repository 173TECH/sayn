from .utils.log_creator import get_data, get_create_table
from sayn import PythonTask


class LoadData(PythonTask):
    def setup(self):
        # we use this list to control how many battles we want per tournament
        self.tournament_battles = [
            {"tournament_id": 1, "n_battles": 1000},
            {"tournament_id": 2, "n_battles": 250},
            {"tournament_id": 3, "n_battles": 500},
        ]

        self.set_run_steps(
            [
                "Generate Data",
                "Load fighters",
                "Load arenas",
                "Load tournaments",
                "Load battles",
            ]
        )

        return self.success()

    def run(self):
        self.start_step("Generate Data")
        data_to_load = get_data(self.tournament_battles)
        self.finish_current_step()

        for log_type, log_data in data_to_load.items():
            self.start_step(f"Load {log_type}")

            # Create table
            q_create = get_create_table(log_type)
            result = self.default_db.execute(q_create)
            if result.is_err:
                return result

            # Load logs
            result = self.default_db.load_data(f"logs_{log_type}", None, log_data)
            if result.is_err:
                return result

            self.finish_current_step()

        return self.success()
