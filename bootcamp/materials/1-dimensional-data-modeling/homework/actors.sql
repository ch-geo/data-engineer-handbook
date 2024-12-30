-- Actors table DDLs
CREATE TYPE film_record AS (
	film TEXT
	, votes INTEGER
	, rating INTEGER
	, filmid TEXT
);


CREATE TYPE actor_rating AS ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE actors (
	actor TEXT
	, actorid TEXT
	, films film_record[]
	, quality_class actor_rating
	, is_active BOOLEAN
  , current_year INTEGER
  , PRIMARY KEY (actorid, current_year)
);
