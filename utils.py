import os

from functools import reduce

collision_fields = [
    "CASE_ID",
    "ACCIDENT_YEAR",
    "COLLISION_DATE",
    "COLLISION_TIME",
    "REPORTING_DISTRICT",
    "DAY_OF_WEEK",
    "CNTY_CITY_LOC",
    "PRIMARY_RD",
    "SECONDARY_RD",
    "DISTANCE",
    "DIRECTION",
    "INTERSECTION",
    "WEATHER_1",
    "WEATHER_2",
    "STATE_HWY_IND",
    "SIDE_OF_HWY",
    "TOW_AWAY",
    "COLLISION_SEVERITY",
    "NUMBER_KILLED",
    "NUMBER_INJURED",
    "PARTY_COUNT",
    "PEDESTRIAN_ACCIDENT",
    "BICYCLE_ACCIDENT",
    "MOTORCYCLE_ACCIDENT",
    "TRUCK_ACCIDENT",
    "NOT_PRIVATE_PROPERTY",
    "ALCOHOL_INVOLVED",
    "COUNT_SEVERE_INJ",
    "COUNT_VISIBLE_INJ",
    "COUNT_COMPLAINT_PAIN",
    "COUNT_PED_KILLED",
    "COUNT_PED_INJURED",
    "COUNT_BICYCLIST_KILLED",
    "COUNT_BICYCLIST_INJURED",
    "COUNT_MC_KILLED",
    "COUNT_MC_INJURED",
    "LATITUDE",
    "LONGITUDE",
]


party_fields = [
    "CASE_ID",
    "PARTY_NUMBER",
    "PARTY_TYPE",
    "AT_FAULT",
    "PARTY_SEX",
    "PARTY_AGE",
    "PARTY_SOBRIETY",
    "PARTY_DRUG_PHYSICAL",
    "DIR_OF_TRAVEL",
    "PARTY_SAFETY_EQUIP_1",
    "PARTY_SAFETY_EQUIP_2",
    "SP_INFO_1",
    "SP_INFO_2",
    "SP_INFO_3",
    "PARTY_NUMBER_KILLED",
    "PARTY_NUMBER_INJURED",
    "MOVE_PRE_ACC",
    "VEHICLE_YEAR",
    "VEHICLE_MAKE",
    "STWD_VEHICLE_TYPE",
    "CHP_VEH_TYPE_TOWING",
    "CHP_VEH_TYPE_TOWED",
    "RACE",
    "INATTENTION",
    "SPECIAL_INFO_F",
    "SPECIAL_INFO_G",
]

victim_fields = [
    "CASE_ID",
    "PARTY_NUMBER",
    "VICTIM_ROLE",
    "VICTIM_SEX",
    "VICTIM_AGE",
    "VICTIM_DEGREE_OF_INJURY",
    "VICTIM_SEATING_POSITION",
    "VICTIM_SAFETY_EQUIP_1",
    "VICTIM_SAFETY_EQUIP_2",
    "VICTIM_EJECTED",
]

collision_filed_rename = {
    "CASE_ID": "case_id",
    "ACCIDENT_YEAR": "accident_year",
    "COLLISION_DATE": "crash_date",
    "COLLISION_TIME": "crash_time",
    "REPORTING_DISTRICT": "reporting_district",
    "DAY_OF_WEEK": "day_of_week",
    "CNTY_CITY_LOC": "county_city_loc",
    "PRIMARY_RD": "primary_road",
    "SECONDARY_RD": "secondary_road",
    "DISTANCE": "distance",
    "DIRECTION": "direction",
    "INTERSECTION": "intersection",
    "WEATHER_1": "weather_1",
    "WEATHER_2": "weather_2",
    "STATE_HWY_IND": "state_high_way_indicator",
    "SIDE_OF_HWY": "side_of_hightway",
    "TOW_AWAY": "tow_away",
    "COLLISION_SEVERITY": "collision_severity",
    "NUMBER_KILLED": "number_killed",
    "NUMBER_INJURED": "number_injured",
    "PARTY_COUNT": "party_count",
    "PEDESTRIAN_ACCIDENT": "pedestrian_accident",
    "BICYCLE_ACCIDENT": "bicycle_accident",
    "MOTORCYCLE_ACCIDENT": "motorcycle_accident",
    "TRUCK_ACCIDENT": "truck_accident",
    "NOT_PRIVATE_PROPERTY": "not_private_property",
    "ALCOHOL_INVOLVED": "alcohol_involved",
    "COUNT_SEVERE_INJ": "count_severe_injury",
    "COUNT_VISIBLE_INJ": "count_visible_injury",
    "COUNT_COMPLAINT_PAIN": "count_complain_pain",
    "COUNT_PED_KILLED": "count_pedestrian_killed",
    "COUNT_PED_INJURED": "count_pedestrian_injured",
    "COUNT_BICYCLIST_KILLED": "count_bucyclist_killed",
    "COUNT_BICYCLIST_INJURED": "count_bucyclist_injured",
    "COUNT_MC_KILLED": "count_motor_cyclist_killed",
    "COUNT_MC_INJURED": "count_motor_cyclist_injured",
    "LATITUDE": "latitude",
    "LONGITUDE": "longitude",
}


def lookup_data_gen(data):
    data_list = [(key, value) for key, value in data.items()]
    return data_list


def get_file_names(directory_path):
    try:
        # Get the list of files in the specified directory
        filenames = os.listdir(directory_path)

        # Print the list of filenames

        return filenames

    except OSError:
        print(f"Error reading directory: {directory_path}")
        return None


def list_to_string(input_list, delimiter):
    result = reduce(lambda x, y: str(x) + delimiter + str(y), input_list)
    return str(result)
