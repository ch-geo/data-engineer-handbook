--  What is the average number of web events of a session from a user on Tech Creator?

SELECT
	session_start,
	ip,
	AVG(num_hits) AS avg_num_hits
FROM processed_session_events
WHERE host = 'bootcamp.techcreator.io'
GROUP BY session_start, ip

--  Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

SELECT
	session_start,
	ip,
	host,
	AVG(num_hits) AS avg_num_hits
FROM processed_session_events
GROUP BY session_start, ip, host
ORDER BY ip, host