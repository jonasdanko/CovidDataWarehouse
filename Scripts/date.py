import psycopg2
import pandas

# DB connection function, pass in username, password...
# Function returns the connection object
def connect(user, password):
	host = "www.eecs.uottawa.ca"
	database = "group_25"
	port = 15432
	return psycopg2.connect(host=host, database=database, user=user, password=password, port=port)


def sql(cursor, command):
	cursor.execute(command)
	conn.commit()


conn = connect("", "")
cursor = conn.cursor()

#file = "../Data/covid_data.csv"
#df = pandas.read_csv(file)
#print(df.head())

key = 0
week_of_year = 36 #First week of Septemeber
year = 2020

#September
for i in range(30):
	day = i+1
	month = 9
	dayof_week = ""
	weekend = False
	holiday = False
	season = "Fall"
	
	if day%7==0:
		dayof_week = "Monday"
	elif day%7==1:
		dayof_week = "Tuesday"
	elif day%7==2:
		dayof_week = "Wednesday"
	elif day%7==3:
		dayof_week = "Thursday"
	elif day%7==4:
		dayof_week = "Friday"
	elif day%7==5:
		dayof_week = "Saturday"
	else:
		dayof_week = "Sunday"


	if dayof_week == "Saturday" or dayof_week == "Sunday":
		weekend = True

	#For holiday adding 7 day buffer after a holiday to account for onset time...
	#Labour day only holdiay in September (the 7th) ....
	if day>=7 and day<=14:
		holiday = True


	command = "INSERT INTO date_dimension(date_key, day, month, year, day_of_week, week_of_year, weekend, season, holiday) VALUES(" + str(key) + "," + str(day) + "," + str(month) + "," + str(year) + ",'" + dayof_week + "'," + str(week_of_year) + "," + str(weekend) + ",'" + season + "'," + str(holiday) + ");"
	sql(cursor, command)

	if dayof_week == "Saturday":
		week_of_year += 1
	key += 1


#October
for i in range(31):
	day = i+1
	month = 10
	dayof_week = ""
	weekend = False
	holiday = False
	season = "Fall"
	
	if day%7==0:
		dayof_week = "Wednesday"
	elif day%7==1:
		dayof_week = "Thursday"
	elif day%7==2:
		dayof_week = "Friday"
	elif day%7==3:
		dayof_week = "Saturday"
	elif day%7==4:
		dayof_week = "Sunday"
	elif day%7==5:
		dayof_week = "Monday"
	else:
		dayof_week = "Tuesday"


	if dayof_week == "Saturday" or dayof_week == "Sunday":
		weekend = True

	#For holiday adding 7 day buffer after a holiday to account for onset time...
	#Thanksgiving and Halloween  holidays in Octbober....
	if day>=12 and day<=19:
		holiday = True
	if day==31:
		holiday = True

	command = "INSERT INTO date_dimension(date_key, day, month, year, day_of_week, week_of_year, weekend, season, holiday) VALUES(" + str(key) + "," + str(day) + "," + str(month) + "," + str(year) + ",'" + dayof_week + "'," + str(week_of_year) + "," + str(weekend) + ",'" + season + "'," + str(holiday) + ");"
	sql(cursor, command)

	if dayof_week == "Saturday":
		week_of_year += 1
	key += 1

#November
for i in range(30):
	day = i+1
	month = 11
	dayof_week = ""
	weekend = False
	holiday = False
	season = "Fall"
	
	if day%7==0:
		dayof_week = "Saturday"
	elif day%7==1:
		dayof_week = "Sunday"
	elif day%7==2:
		dayof_week = "Monday"
	elif day%7==3:
		dayof_week = "Tuesday"
	elif day%7==4:
		dayof_week = "Wednesday"
	elif day%7==5:
		dayof_week = "Thursday"
	else:
		dayof_week = "Friday"


	if dayof_week == "Saturday" or dayof_week == "Sunday":
		weekend = True

	#For holiday adding 7 day buffer after a holiday to account for onset time...
	#Carry over from halloween and Remembrance day only holdiay in November (the 11th) ....
	if day<=7:
		holiday = True
	if day>=11 and day<=18:
		holiday = True


	command = "INSERT INTO date_dimension(date_key, day, month, year, day_of_week, week_of_year, weekend, season, holiday) VALUES(" + str(key) + "," + str(day) + "," + str(month) + "," + str(year) + ",'" + dayof_week + "'," + str(week_of_year) + "," + str(weekend) + ",'" + season + "'," + str(holiday) + ");"
	sql(cursor, command)

	if dayof_week == "Saturday":
		week_of_year += 1
	key += 1

#Decemeber
for i in range(31):
	day = i+1
	month = 12
	dayof_week = ""
	weekend = False
	holiday = False
	season = "Fall"
	
	if day%7==0:
		dayof_week = "Monday"
	elif day%7==1:
		dayof_week = "Tuesday"
	elif day%7==2:
		dayof_week = "Wednesday"
	elif day%7==3:
		dayof_week = "Thursday"
	elif day%7==4:
		dayof_week = "Friday"
	elif day%7==5:
		dayof_week = "Saturday"
	else:
		dayof_week = "Sunday"


	if dayof_week == "Saturday" or dayof_week == "Sunday":
		weekend = True

	#For holiday adding 7 day buffer after a holiday to account for onset time...
	#Christmas and New Years Eve holidays in December
	if day>=25 and day<=31:
		holiday = True

	#Season change also
	if day>=21:
		season = "Winter"

	command = "INSERT INTO date_dimension(date_key, day, month, year, day_of_week, week_of_year, weekend, season, holiday) VALUES(" + str(key) + "," + str(day) + "," + str(month) + "," + str(year) + ",'" + dayof_week + "'," + str(week_of_year) + "," + str(weekend) + ",'" + season + "'," + str(holiday) + ");"
	sql(cursor, command)

	if dayof_week == "Saturday":
		week_of_year += 1
	key += 1

cursor.close()
