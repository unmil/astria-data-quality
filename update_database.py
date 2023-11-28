from tletools import TLE
from tletools.pandas import load_dataframe
import pandas as pd
import glob
from pathlib import Path
import json
from datetime import datetime

def leading_zero_removal(x):
	return x.lstrip("0")

def strip_series_strings(x):
	return x.strip()

def update_JSPOC_database():
	file_list = glob.glob("../../test_JSPOC/TLE/*.tle")
	complete_source_frame = None
	for file_name in file_list:
		source_frame = load_dataframe("../../test_JSPOC/TLE/" + Path(file_name).stem + ".tle")
		source_frame.rename(columns={'norad': 'NORAD_CAT_ID'}, inplace = "True")
		source_frame["NORAD_CAT_ID"] = source_frame["NORAD_CAT_ID"].apply(leading_zero_removal)
		source_frame["NORAD_CAT_ID"] = source_frame["NORAD_CAT_ID"].apply(strip_series_strings).astype('int')
		norad_num_file = "../../test_JSPOC/TLE/satcat-" + Path(file_name).stem + ".json"
		try:
			with open(norad_num_file) as json_file:
				json_data = pd.DataFrame(json.load(json_file))
				json_data["NORAD_CAT_ID"] = json_data["NORAD_CAT_ID"].apply(strip_series_strings).astype('int')
				source_time = source_frame.merge(json_data, how='inner', on='NORAD_CAT_ID')
		except FileNotFoundError:
			source_frame["COUNTRY"] = "N/F"
			source_frame["LAUNCH"] = "N/F"
			
		if complete_source_frame is None:
			complete_source_frame = source_frame
		else:
			pd.concat([complete_source_frame, source_frame])
		
	json_obj = complete_source_frame.to_json(orient = 'records')
	updated_json = {}
	for tuple in json.loads(json_obj):
		updated_json[tuple["NORAD_CAT_ID"]] = tuple
	
	with open('file_name.txt', 'w') as f:
	        f.write(json.dumps(updated_json, indent = 2))

print("Please select any of the following datasets to update:")
print("Type 1 for JSPOC")
dataset = input()
if dataset == "1":
	update_JSPOC_database()


