CREATE TABLE host_activity_reduced (
    host TEXT
    , month DATE
    , hit_array INT []
    , unique_visitors_array INT []
    , PRIMARY KEY (host, month)
);