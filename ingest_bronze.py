import os
from read_data import ReadData
from utils import get_file_names

from _config import DELTA_PATH, SOURCE_PATH


class IngestData(ReadData):
    def __init__(self):
        super().__init__()
        self.ingest_collision_raw_data_delta()
        self.ingest_victim_raw_data_delta()
        self.ingest_party_raw_data_delta()

    def ingest_collision_raw_data_delta(self):
        collision_source_path = SOURCE_PATH + "/collision"
        file_names = get_file_names(collision_source_path)
        for file_name in file_names:
            collision_df = self.read_collision_record_data(
                "{}/{}".format(collision_source_path, file_name)
            )

            collision_df.write.format("delta").mode("append").save(
                DELTA_PATH + "/collision"
            )

    def ingest_victim_raw_data_delta(self):
        victim_source_path = SOURCE_PATH + "/victim"
        file_names = get_file_names(victim_source_path)
        for file_name in file_names:
            collision_df = self.read_collision_record_data(
                "{}/{}".format(victim_source_path, file_name)
            )
            collision_df.write.format("delta").mode("append").save(
                DELTA_PATH + "/victim"
            )

    def ingest_party_raw_data_delta(self):
        party_source_path = SOURCE_PATH + "/party"
        file_names = get_file_names(party_source_path)
        for file_name in file_names:
            collision_df = self.read_collision_record_data(
                "{}/{}".format(party_source_path, file_name)
            )
            collision_df.write.format("delta").mode("append").save(
                DELTA_PATH + "/party"
            )
