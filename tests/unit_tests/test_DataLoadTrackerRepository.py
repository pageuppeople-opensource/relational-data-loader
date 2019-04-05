import unittest
import uuid
import json
import subprocess

from hashlib import md5
from random import randrange
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from rdl.data_load_tracking.DataLoadExecution import DataLoadExecution
from rdl.data_load_tracking.DataLoadTrackerRepository import DataLoadTrackerRepository
from rdl.shared import Constants

TEST_DB = "rdl_unit_test_dest"
PSQL_STRING_FORMAT = "postgresql+psycopg2://{username}:{password}@{server_string}/{db}"

CONFIG_PATH = "./tests/unit_tests/config/"


class TestDataLoadTrackerRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open(CONFIG_PATH + "connection.json", "r", encoding="utf8") as f:
            config_json = json.loads(f.read(), encoding="utf8")
            gen_connection_string = PSQL_STRING_FORMAT.format(**config_json["psql"], db="{db}")

        cls.master_engine = create_engine(gen_connection_string.format(db="postgres"))

        conn = cls.master_engine.connect()
        conn.execute("commit")
        conn.execute(f"CREATE DATABASE {TEST_DB};")
        conn.close()

        subprocess.call(f"alembic -c rdl/alembic.ini -x {gen_connection_string.format(db=TEST_DB)} upgrade head")

        cls.target_engine = create_engine(gen_connection_string.format(db=TEST_DB))
        cls.data_load_tracker = DataLoadTrackerRepository(sessionmaker(bind=cls.target_engine))

    @classmethod
    def tearDownClass(cls):
        cls.target_engine.dispose()

        conn = cls.master_engine.connect()
        conn.execute("commit")
        conn.execute(f"drop database {TEST_DB}")
        conn.close()

    def test_get_results(self):
        self.timestamps = []
        self.fake_session = TestDataLoadTrackerRepository.data_load_tracker.session_maker()
        self.fake_models = [
            {
                "name": "fake_jobs",
                "checksum": uuid.uuid4(),
                "num_rows": 10,
                "sync_version": 1,
            },
            {
                "name": "fake_users",
                "checksum": uuid.uuid4(),
                "num_rows": 666,
                "sync_version": 2,
            },
            {
                "name": "fake_applicants",
                "checksum": uuid.uuid4(),
                "num_rows": 9999,
                "sync_version": 3,
            },
        ]
        last_succesful_time = datetime.now() - timedelta(hours=2)
        self.timestamps.append(last_succesful_time)

        # 1: incremental only of some rows
        frt_1 = []
        incr_1 = ['fake_jobs', 'fake_applicants']
        run_time = last_succesful_time + timedelta(hours=2)
        self.__simulate_rdl_run(run_time, frt_1, incr_1)
        # 1A: ensure full refresh list is empty
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-1])
        self.assertCountEqual(results, frt_1)
        # 1B: ensure incremental list is full
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-1])
        self.assertCountEqual(results, incr_1)
        last_succesful_time = run_time + timedelta(minutes=30)
        self.timestamps.append(last_succesful_time)

        # 2: simulate no changes
        frt_2 = []
        incr_2 = []
        run_time = last_succesful_time + timedelta(hours=2)
        self.__simulate_rdl_run(run_time, frt_2, incr_2)
        # 2A: ensure full refresh list is empty
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-1])
        self.assertCountEqual(results, frt_2)
        # 2B: ensure incremental list is empty
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-1])
        self.assertCountEqual(results, incr_2)
        # 2C: assuming DBT failure, ensure RDL outputs include #1
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-2])
        frt_set = set(frt_1 + frt_2)
        self.assertCountEqual(results, frt_set)
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-2])
        self.assertCountEqual(results, set(incr_1 + incr_2).difference(frt_set))
        last_succesful_time = run_time + timedelta(minutes=30)
        self.timestamps.append(last_succesful_time)

        # 3: simulate Full Refresh only
        frt_3 = ['fake_users', 'fake_applicants']
        incr_3 = []
        run_time = last_succesful_time + timedelta(hours=2)
        self.__simulate_rdl_run(run_time, frt_3, incr_3)
        # 3A: ensure full refresh list is correct
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-1])
        self.assertCountEqual(results, frt_3)
        # 3B: ensure incremental list is empty
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-1])
        self.assertCountEqual(results, incr_3)
        # 3C: assuming DBT failure, ensure RDL outputs include #1 and #2
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-3])
        frt_set = set(frt_1 + frt_2 + frt_3)
        self.assertCountEqual(results, frt_set)
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-3])
        self.assertCountEqual(results, set(incr_1 + incr_2 + incr_3).difference(frt_set))
        # 3D: as #2 was blank, ensure #3 == #2
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-2])
        results_ = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-1])
        self.assertCountEqual(results, results_)
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-2])
        results_ = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-1])
        self.assertCountEqual(results, results_)
        last_succesful_time = run_time + timedelta(minutes=30)
        self.timestamps.append(last_succesful_time)

        # 4: simulate mixed
        frt_4 = ['fake_jobs', 'fake_applicants']
        incr_4 = ['fake_users']
        run_time = last_succesful_time + timedelta(hours=2)
        self.__simulate_rdl_run(run_time, frt_4, incr_4)
        # 4A: ensure full refresh list is correct
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-1])
        self.assertCountEqual(results, frt_4)
        # 4B: ensure incremental list is correct
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-1])
        self.assertCountEqual(results, incr_4)
        # 4C: assuming DBT failure, ensure RDL outputs include #3
        results = TestDataLoadTrackerRepository.data_load_tracker.get_full_refresh_since(self.timestamps[-2])
        frt_set = set(frt_4 + frt_3)
        self.assertCountEqual(results, frt_set)
        results = TestDataLoadTrackerRepository.data_load_tracker.get_only_incremental_since(self.timestamps[-2])
        self.assertCountEqual(results, set(incr_4 + incr_3).difference(frt_set))
        last_succesful_time = run_time + timedelta(minutes=30)
        self.timestamps.append(last_succesful_time)

        self.fake_session.close()

    def __simulate_rdl_run(self, iteration_time, full_refresh_models, incremental_models):

        correlation_id = uuid.uuid4()

        # emulate an execution of RDL
        for j, fake_model in enumerate(self.fake_models):
            execution_time_ms = 10
            time_since_iteration_ms = j * execution_time_ms
            completed_on = iteration_time + timedelta(milliseconds=time_since_iteration_ms)

            full_refresh_reason = Constants.FullRefreshReason.NOT_APPLICABLE
            is_full_refresh = False
            rows_processed = 0
            status = Constants.ExecutionStatus.SKIPPED_AS_ZERO_ROWS
            next_sync_version = fake_model["sync_version"]

            # fake a model change
            if fake_model["name"] in full_refresh_models:
                fake_model["checksum"] = uuid.uuid4()
                is_full_refresh = True
                rows_processed = fake_model["num_rows"]
                full_refresh_reason = Constants.FullRefreshReason.MODEL_CHANGED
                next_sync_version = 0

            # fake a row update/change
            elif fake_model["name"] in incremental_models:
                rows_processed = randrange(1, fake_model["num_rows"])
                status = Constants.ExecutionStatus.COMPLETED_SUCCESSFULLY

                # emulate a random amount of changes from change tracking
                next_sync_version += randrange(1, 10)

            fake_data_load = DataLoadExecution(
                model_name=fake_model["name"],
                is_full_refresh=is_full_refresh,
                last_sync_version=fake_model["sync_version"],
                sync_version=next_sync_version,
                completed_on=completed_on,
                execution_time_ms=execution_time_ms,
                rows_processed=rows_processed,
                correlation_id=correlation_id,
                status=status,
                model_checksum=fake_model["checksum"],
                full_refresh_reason=full_refresh_reason
            )

            self.fake_session.add(fake_data_load)
            fake_model["sync_version"] = next_sync_version

        self.fake_session.commit()


if __name__ == '__main__':
    unittest.main()
