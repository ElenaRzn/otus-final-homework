DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;


CREATE TABLE errors_statistics (
errorType                       varchar(50),
"count(timeReceiveDataForModel)"  bigint,
"min(ignite_response_time)" bigint,
"max(ignite_response_time)" bigint,
"avg(ignite_response_time)" double
);

CREATE TABLE empty_statistics (
"count(event_id)" varchar(50),
"min(processing_time)"  bigint,
"max(processing_time)" bigint,
"avg(processing_time)" bigint,
);

