create database operation_analytics;

use operation_analytics;

#  CASE STUDY 1

#  Uploading the dataset

CREATE TABLE job_data (ds DATE,job_id INT NOT NULL, actor_id INT NOT NULL, event VARCHAR(50) NOT NULL,     
language VARCHAR(15) NOT NULL,  time_spent INT NOT NULL, org CHAR(2) ); 

INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org) 
VALUES ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'), 
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

#  Query 1,Number of jobs reviewed
#  Calculate the number of jobs reviewed per hour per day for November 2020?

SELECT 
    ds AS review_date, 
    COUNT(job_id) AS total_jobs_reviewed,
    SUM(time_spent) / 3600 AS total_hours_spent,
    COUNT(job_id) / (SUM(time_spent) / 3600) AS jobs_per_hour
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30' -- Filter for November 2020
GROUP BY ds
ORDER BY ds;


#  Query 2,Throughput:The no. of events happening per second.
#  Let’s say the above metric is called throughput. Calculate 7 day rolling average of throughput? 
For throughput, do you prefer daily metric or 7-day rolling and why?

select ds, `number of events per day`, 
avg(event_or_events_per_day) over(order by ds rows between 6 preceding and current row) as `7 day rolling avg` 
from (select ds, count(job_id) as `number of events per day`
from job_data
group by ds) as event_s;


#  Explain your throghput preference in ppt

#  Query 3,Percentage share of each language: Share of each language for different contents
# Calculate the percentage share of each language in the last 30 days?

select language as languages, concat(count(*)*100/(select count(*)
from job_data),'%') as percetage_share
from job_data
group by language;


#  Query 4,Duplicate rows: Rows that have the same value present in them
#  Let’s say you see some duplicate rows in the data. How will you display duplicates from the table?

select ds, COUNT(ds) as `Duplicate Rows`
from job_data
group by ds
having no_of_duplicate > 1;


#  CASE STUDY 2
#  Uploading the dataset

Create TABLE events (user_id INT NOT NULL,occurred_at DATE, event_type VARCHAR(50) NOT NULL,event_name VARCHAR(50),
location VARCHAR(50) NOT NULL, device VARCHAR(15) NOT NULL, user_type INT NOT NULL); 


# Objective 1 : Measure the activeness of users on a weekly basis

select week(occurred_at) as weeks, event_type, count(distinct user_id) as `weekly_user_engagement`
from events
where event_type = "engagement"
group by week(occurred_at)
order by week(occurred_at) desc;


# Objective 2 : Analyze the growth of users over time(weekly) for a product.

select year, weeks, `new user`,
`new user`-lag(`new user`) over(order by year, weeks) as `user growth`,
 sum(`new user`) over(order by year, weeks) as `total users`
from(
select count(user_id) as `new user`, 
week(activated_at) as weeks, year(activated_at) as year
from users
group by weeks, year
order by weeks, year) as counting;


# Objective 3 : Analyze the retention of users on a weekly basis

with wy as(
select week(occurred_at) as weeks, count(distinct user_id) as `new user`
from events
where event_type = "signup_flow"
group by weeks),   ou as (
select week(occurred_at) as weeks,
count(distinct user_id) as `old users`
from events
where event_type = "engagement"
group by weeks)
select wy.weeks, (ou.`old users`-wy.`new user`) as `Retained users`
from wy
join ou
on wy.weeks = ou.weeks;


#  Objective 4 : Calculate the weekly engagement per device?

select week(occurred_at) as weeks, count(distinct user_id) as `users`, device
from events
where event_type = "engagement"
group by device, week(occurred_at)
order by count(distinct user_id) desc
limit 10;


#  Users engaging with the email service.

SELECT 
    COUNT(user_id) AS users,
    action,
    (COUNT(user_id) / (SELECT 
            COUNT(user_id)
        FROM
            email_events)) * 100 AS `usage percentage`
FROM
    email_events
GROUP BY action
ORDER BY COUNT(user_id) DESC;

