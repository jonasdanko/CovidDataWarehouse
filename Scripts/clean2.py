import pandas

def clean_date(date):
	return date[:10]

file = "../Data/cleaned_data.csv"
df = pandas.read_csv(file)

df['Case_Reported_Date'] = df['Case_Reported_Date'].apply(clean_date)
df['Specimen_Date'] = df['Specimen_Date'].apply(clean_date)

index = 0
index_to_drop = []
for i in df['Specimen_Date']:
	if i < "2020-09-01" or i >"2020-12-31":
		index_to_drop.append(index)
	index += 1

index = 0
for i in df['Case_Reported_Date']:
	if i < "2020-09-01" or i >"2020-12-31":
		index_to_drop.append(index)
	index += 1

index = 0
for i in df['Reporting_PHU_City']:
	#print(i)
	if (i != "Ottawa") and (i != "Toronto") and (i != "Whitby") and (i != "Oakville") and (i != "Mississauga") and (i != "Newmarket"):
		#print(i)
		index_to_drop.append(index)
	index += 1

index_to_drop.sort()
df_cleaned = df.drop(index_to_drop)

df_cleaned.to_csv("../Data/final_cleaned_data.csv")