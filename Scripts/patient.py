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

file = "../Data/covid_data.csv"
df = pandas.read_csv(file)
#print(df.head())

outbreakRelated = [True, False]
gend = ["female", "male"]

ages = df['Age_Group']
ages_no_dups = []
for i in ages:
	if i not in ages_no_dups:
		ages_no_dups.append(i)
ages_no_dups.sort()
#print(ages_no_dups)

aquisition = df['Case_AcquisitionInfo']
aquisition_no_dups = []
for i in aquisition:
	if i not in aquisition_no_dups:
		aquisition_no_dups.append(i)
aquisition_no_dups.sort()
#print(aquisition_no_dups)

patientKey = 0
for g in gend:
	for a in ages_no_dups:
		for out in outbreakRelated:
			for aqui in aquisition_no_dups:
				command = "INSERT INTO patient_dimension(patient_key, gender, age_group, aquisition_group, outbreak_related) VALUES(" + str(patientKey) + ",'" + g + "','" + a + "','" + aqui  + "'," + str(out) + ");"
				sql(cursor, command)
				patientKey += 1

cursor.close()