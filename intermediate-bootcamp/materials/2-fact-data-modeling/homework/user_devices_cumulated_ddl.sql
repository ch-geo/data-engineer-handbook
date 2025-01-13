CREATE TYPE activity_record AS (
	browser_type TEXT
	, activity_date DATE
);
	
CREATE TABLE user_devices_cumulated (
	user_id NUMERIC 
	, device_activity_datelist activity_record[]
	, current_day DATE
	, PRIMARY KEY (user_id, current_day)
);
