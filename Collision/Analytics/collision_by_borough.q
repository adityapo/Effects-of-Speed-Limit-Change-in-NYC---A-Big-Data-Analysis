USE Final_Project;

DROP TABLE IF EXISTS collisions_by_borough;
CREATE TABLE collisions_by_borough
AS
SELECT borough, year, SUM(collision_at_intersection) as total_collision, SUM(collision_injured_count) as total_injured, SUM(collision_killed_count) as total_killed
FROM collision_data
GROUP BY borough, year;

drop table if exists csv_dump;

create table csv_dump ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/an2426/Speed_Limit_Collisions/output_collision_borough' as
select * from collisions_by_borough;