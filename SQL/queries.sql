#a. Drill down and roll up.

#i For instance, explore the total number of positive cases in your data mart; drill down to a month (Sep 2020), and drill down to a specific day.
SELECT d.day, d.month, d.year, count(*) 
FROM  fact_table as f, date_dimension as d, patient_dimension as p 
WHERE  f.reported_date_key = d.date_surrogate_key AND
	   f.patient_key = p.patient_surrogate_key AND
       d.year = 2020 AND d.month =9
GROUP BY d.day,d.month,d.year
ORDER BY d.day DESC;

#iii For instance, consider all the unresolved cases in Toronto City, roll up to GTA, and roll up to all data in your data mart.
SELECT l.phu_name, count(*) as cases
From phu_location_dimension l, fact_table f
WHERE f.location_key = l.location_surrogate_key AND l.phu_name = 'Ottawa Public Health'
GROUP BY l.phu_name;


#b. Slice

#For instance, provide the number of cases in a specific PHU
SELECT l.province, count(f.resolved)
FROM phu_location_dimension l , fact_table f
WHERE f.resolved = false AND f.location_key = l.location_surrogate_key AND l.city = 'Toronto'
GROUP BY l.province

#Average mobility levels in Ottawa 
SELECT AVG (retail_and_recreation), AVG(grocery_and_pharmacy), AVG (parks), AVG (transit_stations), AVG (workplaces), AVG(residential)
FROM mobility_dimension 
WHERE sub_region_2 = 'Ottawa Division'

#c. Dice 

#Number fatal cases in 2 months in Ottawa and Whitby
SELECT count(CASE WHEN F.fatal THEN 1 END) as "Number Fatal Cases in November/December in Ottawa and Whitby"
FROM fact_table F
WHERE F.onset_date_key>1060 AND (F.location_key=7000 OR F.location_key=7002);


#Number of cases in Ottawa and Whitby after Specific Special Measures
SELECT count(CASE WHEN F.special_measures_key=4000 then 1 end) as "Number of cases in Ottawa and Whitby after Gathering Limit Reduced (Special Measure)", 
count(CASE WHEN F.special_measures_key=4001 then 1 end) as "Number of cases in Ottawa and Whitby after province lockdown (Special Measure)"
FROM fact_table F
WHERE (F.location_key=7000 OR F.location_key=7002);

#d.Combining OLAP operations

#Number of resolved cases per month for each city.
SELECT D.month, L.city, count(CASE WHEN F.resolved THEN 1 END) as "Number Non Fatal Cases Per Month in Major Cities"
FROM fact_table F, date_dimension D, phu_location_dimension L
WHERE F.onset_date_key = D.date_surrogate_key AND F.location_key=L.location_surrogate_key 
GROUP BY L.city, D.month
ORDER BY L.city, D.month;

#Number of cases vs average mobility levels by month 
SELECT d.month ,COUNT(f.onset_date_key)as Cases, AVG (m.retail_and_recreation)as, AVG(m.grocery_and_pharmacy), AVG (m.parks), AVG (m.transit_stations), AVG (m.workplaces), AVG(m.residential)
FROM mobility_dimension as m, date_dimension as d, fact_table as f
WHERE m.sub_region_2 = 'Ottawa Division' AND m.mobility_key=f.mobility_key AND d.date_surrogate_key=f.onset_date_key
GROUP BY d.month
ORDER By d.month

#Number of cases on hot vs cold days 
SELECT COUNT(case when W.daily_high_tempreture >=10 then 1 else null end) as HotWeather,
	   COUNT(case when W.daily_high_tempreture <10 and W.daily_high_tempreture >3 then 1 else null end) as WarmWeather,
       COUNT(case when W.daily_high_tempreture <=3 then 1 else null end) as ColdWeather
FROM weather_dimension as W, fact_table as F 
WHERE W.weather_surrogate_key=F.weather_key

#PART 2 

#a. Iceberg queries

#Top 5 days with highest resolved cases
SELECT  d.day, d.month, l.phu_name, count(*) as resolved_num
FROM  fact_table f, date_dimension d, patient_dimension p, phu_location_dimension l
WHERE f.patient_key = p.patient_surrogate_key AND f.reported_date_key = d.date_surrogate_key
      AND d.year=2020  AND f.resolved = true
      AND f.location_key = l.location_surrogate_key
GROUP BY d.day, d.month, l.phu_name
ORDER BY resolved_num DESC
limit 5

#b.Window queries

#PHU ranked by weekly cases 
SELECT d.week_of_year,p.phu_name,COUNT(f.fatal) as Fatal, RANK() OVER(PARTITION BY d.week_of_year ORDER BY COUNT(f.fatal) DESC) Rank
FROM date_dimension as d, fact_table as f, phu_location_dimension as p
WHERE d.date_surrogate_key = f.onset_date_key AND p.location_surrogate_key = f.location_key AND f.fatal=true
GROUP BY p.phu_name, d.week_of_year
ORDER BY d.week_of_year

#c.Using the window clause

#Number of resolved cases in Ottawa for each month.
SELECT D.month, count(F.resolved) AS "Resolved Cases in Ottawa"
FROM fact_table F, date_dimension D 
WHERE F.reported_date_key=D.date_surrogate_key AND F.location_key=7000
GROUP BY D.month
ORDER BY D.month;