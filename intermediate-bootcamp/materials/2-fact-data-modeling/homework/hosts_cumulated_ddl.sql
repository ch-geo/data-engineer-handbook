CREATE TYPE host_activity_record AS (
	user_id NUMERIC
	, activity_date DATE
);


CREATE TABLE hosts_cumulated (
	host TEXT 
	, host_activity_datelist host_activity_record[]
	, current_day DATE
	, PRIMARY KEY (host, current_day)	
);
