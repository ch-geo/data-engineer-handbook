---- SCD DDL
CREATE TABLE actors_history_scd (
	actor TEXT
	, actorid TEXT
	, quality_class actor_rating
	, is_active BOOLEAN
  	, start_date INTEGER
  	, end_date INTEGER
  	, PRIMARY KEY (actorid, start_date)
);
