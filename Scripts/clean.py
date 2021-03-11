import pandas

def clean_date(date):
	return date[:10]

file = "../Data/covid_data.csv"
df = pandas.read_csv(file)
print(df.head())
df['Accurate_Episode_Date'] = df['Accurate_Episode_Date'].apply(clean_date)

index = 0
index_to_drop = []
for i in df['Accurate_Episode_Date']:
	#print(i)
	if i < "2020-09-01" or i >"2020-12-31":
		#print("Dropping")
		index_to_drop.append(index)
	index += 1

index = 0
for i in df['Specimen_Date']:
	if type(i) is not str:
		index_to_drop.append(index)
	index += 1

index_to_drop.sort()
df_cleaned = df.drop(index_to_drop)

df_cleaned['Case_Reported_Date'] = df_cleaned['Case_Reported_Date'].apply(clean_date)
df_cleaned['Specimen_Date'] = df_cleaned['Specimen_Date'].apply(clean_date)

df_cleaned.to_csv("../Data/cleaned_data.csv")

