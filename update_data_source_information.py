from tletools import TLE
from tletools.pandas import load_dataframe
import pandas as pd
import glob
from pathlib import Path
import json
from datetime import datetime
import os

def leading_zero_removal(x):
        return x.lstrip("0")

def strip_series_strings(x):
        return x.strip()

def verify_date(new_date):
        res = False
        try:
                res = bool(datetime.strptime(new_date, "%Y-%m-%d"))
        except ValueError:
                res = False
        return res

def convert_date_range(start_date, end_date, current_db_information):
        is_a_date = True
        if "start_date" in current_db_information:
                is_a_date = verify_date(current_db_information["start_date"])
        if start_date is None or is_a_date == False:
                return start_date, end_date

        if datetime.strptime(current_db_information["end_date"], "%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d"):
                start_date = current_db_information["end_date"]
        elif datetime.strptime(current_db_information["start_date"], "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
                end_date = current_db_information["start_date"]
        elif datetime.strptime(current_db_information["end_date"], "%Y-%m-%d") > datetime.strptime(start_date, "%Y-%m-%d"):
                start_date = current_db_information["end_date"]
        elif datetime.strptime(current_db_information["start_date"], "%Y-%m-%d") < datetime.strptime(end_date, "%Y-%m-%d"):
                end_date = current_db_information["start_date"]

        return start_date, end_date

def update_date_range(start_date, end_date, data_source, current_db_information):
        if "start_date" not in current_db_information[data_source] or not verify_date(current_db_information[data_source]["start_date"]):
                current_db_information[data_source]["start_date"] = start_date
                current_db_information[data_source]["end_date"] = end_date
        else:
                if datetime.strptime(current_db_information[data_source]["start_date"], "%Y-%m-%d") > datetime.strptime(start_date, "%Y-%m-%d"):
                        current_db_information[data_source]["start_date"] = start_date
                if datetime.strptime(current_db_information[data_source]["end_date"], "%Y-%m-%d") < datetime.strptime(end_date, "%Y-%m-%d"):
                        current_db_information[data_source]["end_date"] = end_date

def isFileNotMerged(start_date, end_date, file_name, current_db_information):
        if start_date is None:
                return ("start_date" not in  current_db_information
                        or datetime.strptime( Path(file_name).stem, "%Y-%m-%d") < datetime.strptime(current_db_information["start_date"], "%Y-%m-%d")
                        or datetime.strptime( Path(file_name).stem, "%Y-%m-%d") > datetime.strptime(current_db_information["end_date"], "%Y-%m-%d"))
        else:
                return ((datetime.strptime( Path(file_name).stem, "%Y-%m-%d") >= datetime.strptime(start_date, "%Y-%m-%d"))
                        and (datetime.strptime( Path(file_name).stem, "%Y-%m-%d") <= datetime.strptime(end_date, "%Y-%m-%d")))

def calculateRunningAvg(duration, frequencies, num_of_frequencies):
        num_of_considered_frequencies = 0
        frequency_sum = 0
        while(num_of_frequencies > 0 and duration >= abs(frequencies[num_of_frequencies - 1])):
                duration -= abs(frequencies[num_of_frequencies - 1])
                frequency_sum += abs(frequencies[num_of_frequencies - 1])
                num_of_frequencies -= 1
                num_of_considered_frequencies += 1

        if duration > 0 and num_of_frequencies > 0:
                num_of_considered_frequencies += 1
                frequency_sum += duration

        return frequency_sum / num_of_considered_frequencies

def update_JSPOC_database(start_date, end_date):
        current_db_information = {}
        updated_json = {}
        try:
                with open("config.json") as json_file:
                        current_db_information = json.load(json_file)
        except FileNotFoundError:
                print("Error: Could not load metadata file. Exiting....")
                return

        if "JSPOC" not in current_db_information or "source_file_directory" not in current_db_information["JSPOC"]:
                print("Error: Missing data source location. Exiting....")
                return

        if "created_database_name" not in current_db_information["JSPOC"] or current_db_information["JSPOC"]["created_database_name"] == "":
                current_db_information["JSPOC"]["created_database_name"] = "updated_database.json"

        file_list = glob.glob(current_db_information["JSPOC"]["source_file_directory"] + "*.tle")
        file_list = sorted(file_list)
        # start_date, end_date = convert_date_range(start_date, end_date, current_db_information["JSPOC"])
        current_lowest_date = "9999-12-31"
        current_highest_date = "1990-01-01"
        try:
                with open(current_db_information["JSPOC"]["created_database_name"]) as updated_database_file:
                        updated_json = json.load(updated_database_file)
        except FileNotFoundError:
               print("First time running for database for this filename. If this is not the first run, then please stop the run by cressing Ctrl + C and rename the old file according to the new name saved to the config file")

        for file_name in file_list:
                if (isFileNotMerged(start_date, end_date, file_name, current_db_information["JSPOC"])):
                        print(file_name)
                        if datetime.strptime(current_lowest_date, "%Y-%m-%d") > (datetime.strptime(Path(file_name).stem, "%Y-%m-%d")):
                                current_lowest_date = Path(file_name).stem
                        if datetime.strptime(current_highest_date, "%Y-%m-%d") < (datetime.strptime(Path(file_name).stem, "%Y-%m-%d")):
                                current_highest_date = Path(file_name).stem

                        source_frame = load_dataframe(current_db_information["JSPOC"]["source_file_directory"] + Path(file_name).stem + ".tle")
                        source_frame.rename(columns={'norad': 'NORAD_CAT_ID'}, inplace = "True")
                        source_frame["NORAD_CAT_ID"] = source_frame["NORAD_CAT_ID"].apply(leading_zero_removal)
                        source_frame["NORAD_CAT_ID"] = source_frame["NORAD_CAT_ID"].apply(strip_series_strings)
                        # norad_num_file = current_db_information["JSPOC"]["source_file_directory"] + "satcat-" + Path(file_name).stem + ".json"
                        # try:
                        #         with open(norad_num_file) as json_file:
                        #                 json_data = pd.DataFrame(json.load(json_file))
                        #                 json_data["NORAD_CAT_ID"] = json_data["NORAD_CAT_ID"].apply(strip_series_strings).astype('int')
                        #                 source_frame = source_frame.merge(json_data, how='inner', on='NORAD_CAT_ID')
                        # except FileNotFoundError:
                        #         source_frame["COUNTRY"] = "N/F"
                        #         source_frame["LAUNCH"] = "N/F"

                        unprocessed_records = dict.fromkeys(updated_json.keys(), True)

                        json_obj = source_frame.to_json(orient = 'records')

                        for tuple in json.loads(json_obj):
                                if tuple["NORAD_CAT_ID"] in unprocessed_records.keys():
                                        existing_record = updated_json[tuple["NORAD_CAT_ID"]]
                                        if existing_record["frequencies"][existing_record["num_of_frequencies"] - 1] < 1:
                                                existing_record["latest_epoch_year"] = tuple["epoch_year"]
                                                existing_record["latest_epoch_day"] = tuple["epoch_day"]
                                                existing_record["frequencies"].append(1)
                                                existing_record["avg_frequency"] = ((existing_record["avg_frequency"] * existing_record["num_of_frequencies"]) + 1) / (existing_record["num_of_frequencies"] + 1)
                                        elif existing_record["latest_epoch_year"] != tuple["epoch_year"] or existing_record["latest_epoch_day"] != tuple["epoch_day"]:
                                                existing_record["frequencies"].append(1)
                                                existing_record["latest_epoch_year"] = tuple["epoch_year"]
                                                existing_record["latest_epoch_day"] = tuple["epoch_day"]
                                                existing_record["avg_frequency"] = ((existing_record["avg_frequency"] * existing_record["num_of_frequencies"]) + 1) / (existing_record["num_of_frequencies"] + 1)
                                                existing_record["num_of_frequencies"] += 1
                                        else:
                                                existing_record["frequencies"][existing_record["num_of_frequencies"] - 1] += 1
                                                existing_record["avg_frequency"] = ((existing_record["avg_frequency"] * existing_record["num_of_frequencies"]) + 1) / (existing_record["num_of_frequencies"])
                                else:
                                        unprocessed_records[tuple["NORAD_CAT_ID"]] = True
                                        current_tuple = {
                                                "name": tuple["name"],
                                                "first_epoch_year": tuple["epoch_year"],
                                                "first_epoch_day": tuple["epoch_day"],
                                                "latest_epoch_year": tuple["epoch_year"],
                                                "latest_epoch_day": tuple["epoch_day"],
                                        }
                                        updated_json[tuple["NORAD_CAT_ID"]] = current_tuple
                                        updated_json[tuple["NORAD_CAT_ID"]]["frequencies"] = [1]
                                        updated_json[tuple["NORAD_CAT_ID"]]["num_of_frequencies"] = 1
                                        updated_json[tuple["NORAD_CAT_ID"]]["avg_frequency"] = 1

                                
                                updated_json[tuple["NORAD_CAT_ID"]]["running_avg_past_thirty_days"] = calculateRunningAvg(30, 
                                                updated_json[tuple["NORAD_CAT_ID"]]["frequencies"], updated_json[tuple["NORAD_CAT_ID"]]["num_of_frequencies"])
                                unprocessed_records[tuple["NORAD_CAT_ID"]] = False

                        for norad_cat_id in unprocessed_records:
                                if unprocessed_records[norad_cat_id] == True:
                                        if updated_json[norad_cat_id]["num_of_frequencies"] > 0 and updated_json[norad_cat_id]["frequencies"][updated_json[norad_cat_id]["num_of_frequencies"] - 1] < 1:
                                                updated_json[norad_cat_id]["frequencies"][updated_json[norad_cat_id]["num_of_frequencies"] - 1] -= 1
                                                existing_record["avg_frequency"] = ((existing_record["avg_frequency"] * existing_record["num_of_frequencies"]) + 1) / (existing_record["num_of_frequencies"])
                                        else:
                                                updated_json[norad_cat_id]["frequencies"].append(-1)
                                                updated_json[norad_cat_id]["num_of_frequencies"] += 1
                                                existing_record["avg_frequency"] = ((existing_record["avg_frequency"] * existing_record["num_of_frequencies"]) + 1) / (existing_record["num_of_frequencies"] + 1)
                                        
                                        updated_json[norad_cat_id]["running_avg_past_thirty_days"] = calculateRunningAvg(30, 
                                                updated_json[norad_cat_id]["frequencies"], updated_json[norad_cat_id]["num_of_frequencies"])


        update_date_range(current_lowest_date, current_highest_date, "JSPOC", current_db_information)

        with open(current_db_information["JSPOC"]["created_database_name"], 'w') as f:
                f.write(json.dumps(updated_json))
        with open('config.json', 'w') as f:
                f.write(json.dumps(current_db_information))

# Main Function
print("Please select any of the following datasets to update:")
print("Type 1 for JSPOC")
dataset = input()
print("Please select how do you want to update the datset")
print("Type 1 if you want to provide a range of date to update for the selected dataset, Type 2 if it is a normal update")
is_normal_dataset = input()
start_date = None
end_date = None
if is_normal_dataset == "1":
        print("Please input the starting date in the format of YYYY-MM-DD")
        start_date = input()
        while(not verify_date(start_date)):
                print("Invalid date. Please input the starting date in the format of YYYY-MM-DD")
                start_date = input()
        print("Please input the ending date in the format of YYYY-MM-DD")
        end_date = input()
        while(not verify_date(end_date)):
                print("Invalid date. Please input the ending date in the format of YYYY-MM-DD")
                end_date = input()
if dataset == "1":
        update_JSPOC_database(start_date, end_date)
