#Create functions to query each dimension

import psycopg2
import pandas

# DB connection function, pass in username, password...
# Function returns the connection object
def connect(user, password):
	host = "www.eecs.uottawa.ca"
	database = "group_25"
	port = 15432
	return psycopg2.connect(host=host, database=database, user=user, password=password, port=port)


def sql_select(cursor, command):
	cursor.execute(command)
	key = cursor.fetchone()
	return key

def sql_insert(cursor, command):
	cursor.execute(command)
	conn.commit()


def date(cursor, month, day):
	command = "SELECT date_surrogate_key FROM date_dimension WHERE day=" + str(day) + " AND month=" + str(month) + ";"
	date_key_tuple = sql_select(cursor, command)
	date_key = date_key_tuple[0]
	return date_key

def patient(cursor, age, gender, aquisition, outbreak):
	command = "SELECT patient_surrogate_key FROM patient_dimension WHERE age_group='" + age + "' AND gender= '" +gender+ "' AND aquisition_group= '" + aquisition + "' AND outbreak_related=" +str(outbreak)+ ";"
	patient_tuple = sql_select(cursor, command)
	patient_key = patient_tuple[0]
	return patient_key

def weather(cursor, station, date):
	if station == "Whitby":
		station = "OSHAWA"
	elif station == "Oakville":
		station = "OAKVILLE TWN"
	elif station == "Ottawa":
		station = "OTTAWA INTL A"
	elif station == "Toronto":
		station = "TORONTO INTL A"
	elif station == "Newmarket":
		station = "KING CITY NORTH"
	else:
		station = "TORONTO CITY CENTRE"


	command = "SELECT weather_surrogate_key FROM weather_dimension WHERE station_name='" + station + "' AND date='" + date + "';"
	weather_tuple = sql_select(cursor, command)
	weather_key = weather_tuple[0]
	return weather_key

def mobility(cursor, region, date):
	if region == "Ottawa":
		region = "Ottawa Division"
	elif region == "Toronto":
		region = "Toronto Division"
	elif region == "Newmarket":
		region = "Regional Municipality of York"
	elif region == "Oakville":
		region = "Regional Municipality of Halton"
	elif region == "Whitby":
		region = "Regional Municipality of Durham"
	else:
		region = "Regional Municipality of Peel"


	command = "SELECT mobility_key FROM mobility_dimension WHERE Sub_Region_2='" + region + "' AND Date='" + date + "';"
	mobility_tuple = sql_select(cursor, command)
	mobility_key = mobility_tuple[0]
	return mobility_key


def special_measures(cursor, date):
	sm_key = 4000
	if date >= "2020-09-17" and date <= "2020-09-25":
		sm_key = 4000
	if date >= "2020-09-26" and date <= "2020-10-09":
		sm_key = 4001
	if date >= "2020-10-10" and date <= "2020-11-20":
		sm_key = 4002
	if date >= "2020-11-21" and date <= "2020-12-18":
		sm_key = 4003
	if date >= "2020-12-18":
		sm_key = 4004
	return sm_key

def location(cursor, city):
	command = "SELECT location_surrogate_key FROM phu_location_dimension WHERE city='" + city + "';"
	location_tuple = sql_select(cursor, command)
	location_key = location_tuple[0]
	return location_key



conn = connect("", "")
cursor = conn.cursor()

file = "../Data/final_cleaned_data.csv"
df = pandas.read_csv(file)


#weatherKey = weather(cursor, "TORONTO INTL A", "2020-09-01")
#mobilityKey = mobility(cursor, "Toronto Division", "2020-09-01")


for i in range(8400, len(df)):
	print(str(i) + "/" + str(len(df)))
	row = df.iloc[i]

	onset_date = row[4]
	onset_month = onset_date[5] + onset_date[6]
	onset_day = onset_date[8] + onset_date[9]


	reported_date = row[5]
	reported_month = reported_date[5] + reported_date[6]
	reported_day = reported_date[8] + reported_date[9]

	test_date = row[5]
	test_month = test_date[5] + test_date[6]
	test_day = test_date[8] + test_date[9]

	specimen_date = row[7]
	specimen_month = specimen_date[5] + specimen_date[6]
	specimen_day = specimen_date[8] + specimen_date[9]

	age = row[8]

	gender = row[9]
	gender = gender[:1].lower()

	if gender != "f" and gender !="m":
		if i%2==0:
			gender = "f"
		else:
			gender = "m"

	aquisition = row[10]

	outcome = row[11]
	if outcome == "Resolved":
		resolved = True
		fatal = False
	elif outcome == "Not Resolved":
		resolved = False
		fatal = False
	else:
		resolved = True
		fatal = True

	outbreak = row[12]
	if outbreak == "Yes":
		outbreak = True
	else: outbreak = False

	city = row[16]

	print(age, gender, aquisition, outbreak)

	patientKey = patient(cursor, age, gender, aquisition, outbreak)
	onset_dateKey = date(cursor, onset_month, onset_day)
	reported_dateKey = date(cursor, reported_month, reported_day)
	test_dateKey = date(cursor, test_month, test_day)
	specimen_dateKey = date(cursor, specimen_month, specimen_day)
	specialMeasuresKey = special_measures(cursor, onset_date)
	locationKey = location(cursor, city)
	mobilityKey = mobility(cursor, city, onset_date)
	weatherKey = weather(cursor, city, onset_date)


	command = "INSERT INTO fact_table (onset_date_key, reported_date_key, test_date_key, specimen_date_key, patient_key, location_key, mobility_key, special_measures_key, weather_key, resolved, fatal) VALUES(" + str(onset_dateKey) + ","  + str(reported_dateKey) + "," + str(test_dateKey) + "," + str(specimen_dateKey) + "," + str(patientKey) + "," + str(locationKey) + "," + str(mobilityKey) + "," + str(specialMeasuresKey) + "," + str(weatherKey) + "," + str(resolved) + "," + str(fatal) + ");"
	sql_insert(cursor, command)


cursor.close()
