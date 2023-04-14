
from tletools import TLE
from tletools.pandas import load_dataframe
import pandas as pd
import json
import country_converter as coco
from datetime import datetime
import dateutil.parser

def leading_zero_removal(x):
	return x.lstrip("0").strip()

def compare_data(source1, source2, mismatch):
	if(source1[0] != source2[0]):
		mismatch.append('name')
	if(abs(source1[1] - source2[1]) > 0.01):
                mismatch.append('inclination')
	if(abs(source1[2] - source2[2]) > 0.0001):
                mismatch.append('eccentricity')
	if(pd.Timestamp(source1[3]).to_pydatetime().date() != datetime.strptime(source2[3], '%Y-%m-%d').date()):
                mismatch.append('launch date')
	if(source1[4] != source2[4]):
                mismatch.append('country')
	return mismatch

ucs_file_name = "../../test_UCS/UCS_Database_20220101.xls"
jspoc_file_name = "../../test_JSPOC/TLE/2022-01-01.tle"
satcat_file_name = "../../test_JSPOC/TLE/satcat-2022-01-01.json"

ucs_df = pd.read_excel(ucs_file_name)
ucs_df[['Detailed Purpose', 'Country/Org of UN Registry', 'Type of Orbit', 'Contractor', 'Country of Contractor', 'Launch Site']] = ucs_df[['Detailed Purpose', 'Country/Org of UN Registry', 'Type of Orbit', 'Contractor', 'Country of Contractor', 'Launch Site']].fillna(value='')

ucs_df[['Period (minutes)', 'Launch Mass (kg.)', 'Dry Mass (kg.)', 'Power (watts)', 'Expected Lifetime (yrs.)']] = ucs_df[['Period (minutes)', 'Launch Mass (kg.)', 'Dry Mass (kg.)', 'Power (watts)', 'Expected Lifetime (yrs.)']].fillna(value=-999999999999)
ucs_df['NORAD Number'] = ucs_df['NORAD Number'].astype('int')

jspoc_df = load_dataframe(jspoc_file_name)
satcat_df = pd.DataFrame(json.load(open(satcat_file_name)))

jspoc_df.rename(columns={'norad': 'NORAD_CAT_ID', 'ecc': 'Eccentricity', 'inc': 'Inclination (degrees)'}, inplace = "True")
jspoc_df["NORAD_CAT_ID"] = jspoc_df["NORAD_CAT_ID"].apply(leading_zero_removal).astype('int')
jspoc_df["name"] = jspoc_df["name"].apply(leading_zero_removal)
satcat_df["NORAD_CAT_ID"] = satcat_df["NORAD_CAT_ID"].astype('int')
jspoc_df = jspoc_df.merge(satcat_df, how='inner', on='NORAD_CAT_ID')

#test = ucs_df['Country/Org of UN Registry'].append(jspoc_df['COUNTRY']).unique()
#for name in test:
#	if(coco.convert(name) == 'not found'):
#		print(name)

cc = coco.CountryConverter()
jspoc_df['COUNTRY'] = cc.pandas_convert(series=jspoc_df['COUNTRY'], to='name_short', not_found=None)
ucs_df['Country/Org of UN Registry'] = cc.pandas_convert(series=ucs_df['Country/Org of UN Registry'], to='name_short', not_found=None)

ucs_data = {}
jspoc_data = {}

for row in ucs_df.itertuples():
        ucs_data[row[ucs_df.columns.get_loc("NORAD Number") + 1]] = [row[ucs_df.columns.get_loc('Current Official Name of Satellite') + 1],row[ucs_df.columns.get_loc('Inclination (degrees)') + 1], row[ucs_df.columns.get_loc('Eccentricity') + 1], row[ucs_df.columns.get_loc('Date of Launch') + 1], row[ucs_df.columns.get_loc('Country of Operator/Owner') + 1]]

for row in jspoc_df.itertuples():
        jspoc_data[row[jspoc_df.columns.get_loc("NORAD_CAT_ID") + 1]] = [row[jspoc_df.columns.get_loc('name') + 1], row[jspoc_df.columns.get_loc('Inclination (degrees)') + 1], row[jspoc_df.columns.get_loc('Eccentricity') + 1], row[jspoc_df.columns.get_loc('LAUNCH') + 1], row[jspoc_df.columns.get_loc('COUNTRY') + 1]]

norad_ids = list(ucs_data.keys()) + (list(jspoc_data.keys()))
temp = []
[temp.append(x) for x in norad_ids if x not in temp]
norad_ids = temp
temp = None

complete_match = []
partial_match = {}
discrepency = {}
mismatch = []

for norad_id in norad_ids:
	mismatch = []
	if(norad_id in ucs_data.keys() and norad_id in jspoc_data.keys()):
		mismatch = compare_data(ucs_data[norad_id], jspoc_data[norad_id], mismatch)
		if(len(mismatch) > 0):
			partial_match[norad_id] = mismatch
		else:
			complete_match.append(norad_id)	
	else:
		discrepency[norad_id] = ucs_file_name if norad_id in ucs_data else jspoc_file_name	

res = json.dumps({'complete match': complete_match, 'partial match': partial_match, 'discrepency': discrepency}, indent = 4)
print("Total unique satellites: {0}".format(len(norad_ids)))
print("Complete match: {0}[{1}%]".format(len(complete_match), len(complete_match)*100/len(norad_ids)))
print("Partial match: {0}[{1}%]".format(len(partial_match.keys()), len(partial_match.keys())*100/len(norad_ids)))
print("Discrepency: {0}[{1}%]".format(len(discrepency.keys()), len(discrepency.keys())*100/len(norad_ids)))
with open("ucs_jspoc_compare.json", 'w') as outfile:
	outfile.write(res)
	
