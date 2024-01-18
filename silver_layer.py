from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    TimestampType,
)

from base import Base
from read_data import ReadDelta
from _config import DELTA_PATH
from utils import collision_fields, victim_fields, party_fields, list_to_string


COLLISION_TABLE = "collision"
VICTIM_TABLE = "victim"
PARTY_TABLE = "party"


# class DataCleaning(Base):
#     def rename_fields(self, df=None, fields={}, *args, **kwargs):
#         if df:
#             for key, val in fields.items():
#                 df = df.withColumnRenamed(key, val)
#             return df

#         else:
#             raise DataFrameNoteFound

#     def handle_null(self, df=None, fields={}):
#         if df:
#             for key, val in fields.items():
#                 df.fillna({key: val})
#         else:
#             raise DataFrameNoteFound


class ProcessingSilverLayer(ReadDelta):
    def __init__(self):
        super().__init__()
        self.clean_collision_data()
        self.clean_victim_data()
        self.clean_party_data()

    def clean_collision_data(self):
        path = DELTA_PATH + "/collision"
        self.read_delta_data(table_name=COLLISION_TABLE, path=path)
        fields = list_to_string(input_list=collision_fields, delimiter=",")
        raw_df = self.spark.sql(f"select {fields} from {COLLISION_TABLE}")

        renamed_col_df = raw_df.select(
            col("CASE_ID").cast(IntegerType()).alias("case_id"),
            col("ACCIDENT_YEAR").cast(IntegerType()).alias("accident_year"),
            col("COLLISION_DATE").cast(DateType()).alias("crash_date"),
            col("COLLISION_TIME").cast(TimestampType()).alias("crash_time"),
            col("REPORTING_DISTRICT").alias("reporting_district"),
            col("DAY_OF_WEEK").cast(IntegerType()).alias("day_of_week"),
            col("CNTY_CITY_LOC").alias("county_city_loc"),
            col("PRIMARY_RD").alias("primary_road"),
            col("SECONDARY_RD").alias("secondary_road"),
            col("DISTANCE").alias("distance"),
            col("DIRECTION").alias("direction"),
            col("INTERSECTION").alias("intersection"),
            col("WEATHER_1").alias("weather_1"),
            col("WEATHER_2").alias("weather_2"),
            col("STATE_HWY_IND").alias("state_high_way_indicator"),
            col("SIDE_OF_HWY").alias("side_of_hightway"),
            col("TOW_AWAY").alias("tow_away"),
            col("COLLISION_SEVERITY").alias("collision_severity"),
            col("NUMBER_KILLED").cast(IntegerType()).alias("number_killed"),
            col("NUMBER_INJURED").cast(IntegerType()).alias("number_injured"),
            col("PARTY_COUNT").cast(IntegerType()).alias("party_count"),
            col("PEDESTRIAN_ACCIDENT").alias("pedestrian_accident"),
            col("BICYCLE_ACCIDENT").alias("bicycle_accident"),
            col("MOTORCYCLE_ACCIDENT").alias("motorcycle_accident"),
            col("TRUCK_ACCIDENT").alias("truck_accident"),
            col("NOT_PRIVATE_PROPERTY").alias("not_private_property"),
            col("ALCOHOL_INVOLVED").alias("alcohol_involved"),
            col("COUNT_SEVERE_INJ").cast(IntegerType()).alias("count_severe_injury"),
            col("COUNT_VISIBLE_INJ").cast(IntegerType()).alias("count_visible_injury"),
            col("COUNT_COMPLAINT_PAIN")
            .cast(IntegerType())
            .alias("count_complain_pain"),
            col("COUNT_PED_KILLED")
            .cast(IntegerType())
            .alias("count_pedestrian_killed"),
            col("COUNT_BICYCLIST_KILLED").cast(IntegerType()).alias("count_byc_killed"),
            col("COUNT_MC_KILLED").cast(IntegerType()).alias("count_moto_c_killed"),
            col("COUNT_PED_INJURED").cast(IntegerType()).alias("count_pedestrian_inju"),
            col("COUNT_MC_INJURED").cast(IntegerType()).alias("count_moto_c_inju"),
            col("COUNT_BICYCLIST_INJURED").cast(IntegerType()).alias("count_byc_inju"),
            col("LATITUDE").alias("latitude"),
            col("LONGITUDE").alias("longitude"),
        )

        fill_null_df = renamed_col_df.fillna(
            {
                "case_id": "deprecated",
                "motorcycle_accident": "no info",
                "pedestrian_accident": "no info",
                "bicycle_accident": "no infor",
                "truck_accident": "no info",
                "alcohol_involved": "no info",
                "state_high_way_indicator": "no info",
                "side_of_hightway": "no info",
                "count_severe_injury": 0,
                "count_visible_injury": 0,
                "count_complain_pain": 0,
                "count_pedestrian_killed": 0,
                "count_byc_killed": 0,
                "count_moto_c_killed": 0,
                "count_pedestrian_inju": 0,
                "count_byc_inju": 0,
                "count_moto_c_inju": 0,
            }
        )

        fill_null_df.createOrReplaceTempView("collision_temp_view")

        collision_header_df = self.spark.sql(
            "select case_id,accident_year,crash_date,crash_time,reporting_district,day_of_week,county_city_loc,primary_road,secondary_road,distance,direction,intersection from collision_temp_view"
        )

        collision_detail_df = self.spark.sql(
            "select case_id,party_count,pedestrian_accident,bicycle_accident,motorcycle_accident,truck_accident,alcohol_involved,weather_1,weather_2,state_high_way_indicator,side_of_hightway from collision_temp_view"
        )

        collision_injury_death_df = self.spark.sql(
            "select case_id,count_severe_injury,count_visible_injury,count_complain_pain,count_pedestrian_killed,count_byc_killed,count_moto_c_killed,count_pedestrian_inju,count_byc_inju,count_moto_c_inju from collision_temp_view"
        )
        collision_header_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/collision_header"
        )
        collision_detail_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/collision_detail"
        )
        collision_injury_death_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/collision_injury_death"
        )

    def clean_victim_data(self):
        path = DELTA_PATH + "/victim"
        self.read_delta_data(table_name=VICTIM_TABLE, path=path)
        fields = list_to_string(input_list=victim_fields, delimiter=",")
        victim_raw_df = self.spark.sql(f"select {fields} from {VICTIM_TABLE}")
        renamed_vict_df = victim_raw_df.select(
            col("CASE_ID").cast(IntegerType()).alias("case_id"),
            col("PARTY_NUMBER").alias("party_number"),
            col("VICTIM_ROLE").alias("victim_role"),
            col("VICTIM_SEX").alias("victim_sex"),
            col("VICTIM_AGE").alias("victim_age"),
            col("VICTIM_DEGREE_OF_INJURY").alias("victim_degree_of_injury"),
            col("VICTIM_SEATING_POSITION").alias("victim_seating_position"),
            col("VICTIM_SAFETY_EQUIP_1").alias("victim_safety_equip_1"),
            col("VICTIM_SAFETY_EQUIP_2").alias("victim_safety_equip_2"),
            col("VICTIM_EJECTED").alias("victim_ejected"),
        )

        fill__vict_null_df = renamed_vict_df.fillna(
            {
                "case_id": "deprecated",
                "party_number": "no info",
                "victim_role": "no info",
                "victim_sex": "no infor",
                "victim_age": "no info",
                "victim_degree_of_injury": "no info",
                "victim_seating_position": "no info",
                "victim_safety_equip_1": 0,
                "victim_safety_equip_2": 0,
                "victim_ejected": 0,
            }
        )

        fill__vict_null_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/victim_info"
        )
        # renamed_col_df.show()

    def clean_party_data(self):
        path = DELTA_PATH + "/party"
        self.read_delta_data(table_name=PARTY_TABLE, path=path)
        fields = list_to_string(input_list=party_fields, delimiter=",")
        party_raw_df = self.spark.sql(f"select {fields} from {PARTY_TABLE}")

        renamed_party_df = party_raw_df.select(
            col("CASE_ID").alias("case_id"),
            col("PARTY_NUMBER").alias("party_num"),
            col("PARTY_TYPE").alias("party_type"),
            col("AT_FAULT").alias("at_fault"),
            col("PARTY_SEX").alias("sex"),
            col("PARTY_AGE").alias("age"),
            col("PARTY_DRUG_PHYSICAL").alias("drug_phy"),
            col("DIR_OF_TRAVEL").alias("dir_of_travel"),
            col("PARTY_SAFETY_EQUIP_1").alias("safety_equip_1"),
            col("PARTY_SAFETY_EQUIP_2").alias("safety_equip_2"),
            col("SP_INFO_1").alias("sp_info_1"),
            col("SP_INFO_2").alias("sp_info_2"),
            col("SP_INFO_3").alias("sp_info_3"),
            col("PARTY_NUMBER_KILLED").alias("number_kill"),
            col("PARTY_NUMBER_INJURED").alias("number_inju"),
            col("MOVE_PRE_ACC").alias("move_pre_acc"),
            col("VEHICLE_YEAR").alias("veh_year"),
            col("VEHICLE_MAKE").alias("veh_make"),
            col("INATTENTION").alias("inattention"),
        )

        fill_party_null_df = renamed_party_df.fillna(
            {
                "case_id": "deprecated",
                "party_num": "no info",
                "party_type": "no info",
                "at_fault": "no infor",
                "sex": "no info",
                "age": "no info",
                "drug_phy": "no info",
                "dir_of_travel": "no info",
                "safety_equip_1": 0,
                "safety_equip_2": 0,
                "sp_info_1": 0,
                "sp_info_2": 0,
                "sp_info_3": 0,
                "number_kill": 0,
                "number_inju": 0,
                "move_pre_acc": 0,
                "veh_year": 0,
                "veh_make": 0,
                "inattention": 0,
            }
        )

        fill_party_null_df.write.format("delta").mode("append").save(
            DELTA_PATH + "/silver/party_info"
        )
