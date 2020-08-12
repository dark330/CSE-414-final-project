-- DROP TABLE IF EXISTS FLIGHTS;
-- DROP TABLE IF EXISTS CARRIERS;
-- DROP TABLE IF EXISTS MONTHS;
-- DROP TABLE IF EXISTS WEEKDAYS;

DROP TABLE IF EXISTS RESERVATIONS;
DROP TABLE IF EXISTS ITINFLIGHTS;
DROP TABLE IF EXISTS ITINERARIES;
DROP TABLE IF EXISTS USERS;

-- CREATE TABLE CARRIERS (
--     cid varchar(7) PRIMARY KEY,
--     name varchar(83)
-- );
--
-- CREATE TABLE MONTHS (
--     mid int PRIMARY KEY,
--     month varchar(9)
-- );
--
-- CREATE TABLE WEEKDAYS (
--     did int PRIMARY KEY,
--     day_of_week varchar(9)
-- );

-- CREATE TABLE FLIGHTS (
--     fid int PRIMARY KEY,
--     month_id int REFERENCES MONTHS (mid),        -- 1-12
--     day_of_month int,    -- 1-31
--     day_of_week_id int REFERENCES WEEKDAYS (did),  -- 1-7, 1 = Monday, 2 = Tuesday, etc
--     carrier_id varchar(7) REFERENCES CARRIERS (cid),
--     flight_num int,
--     origin_city varchar(34),
--     origin_state varchar(47),
--     dest_city varchar(34),
--     dest_state varchar(46),
--     departure_delay int, -- in mins
--     taxi_out int,        -- in mins
--     arrival_delay int,   -- in mins
--     canceled int,        -- 1 means canceled
--     actual_time int,     -- in mins
--     distance int,        -- in miles
--     capacity int,
--     price int            -- in $
-- );


CREATE TABLE USERS (
    username varchar(20) PRIMARY KEY,
    password_hash varbinary(max),
    password_salt varbinary(max),
    balance int
);

--CREATE TABLE ITINERARIES(
--    Lid int PRIMARY KEY,
--    direct varbinary(1)
--);

--CREATE TABLE ITINFLIGHTS (
--    Lid int REFERENCES ITINERARIES (Lid),
--    fid int REFERENCES FLIGHTS (fid),
--    username varchar(20) REFERENCES USERS (username)
--);

CREATE TABLE RESERVATIONS (
    Rid int NOT NULL IDENTITY(1,1) PRIMARY KEY,
--     Lid int REFERENCES ITINERARIES (Lid),
    username varchar(20) REFERENCES USERS (username),
    paid int,
    canceled int,
    date int,
    fid1 int,
    fid2 int
);

--CREATE TABLE RESERVE_FLIGHTS (
--    Rid int REFERENCES RESERVATIONS (Rid),
--    fid int REFERENCES FLIGHTS (fid)
--)



-- ============================ Utility ===============================================
-- DROP EXTERNAL DATA SOURCE IF EXISTS cse414blob;

-- CREATE EXTERNAL DATA SOURCE cse414blob
--     WITH (  TYPE = BLOB_STORAGE,
--     LOCATION = 'https://cse414.blob.core.windows.net/flights'
--     );
--
-- bulk insert Carriers from 'carriers.csv'
--     with (ROWTERMINATOR = '0x0a',
--     DATA_SOURCE = 'cse414blob', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
-- FIRSTROW=1,TABLOCK);
--
-- bulk insert Months from 'months.csv'
--     with (ROWTERMINATOR = '0x0a',
--     DATA_SOURCE = 'cse414blob', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
-- FIRSTROW=1,TABLOCK);
--
-- bulk insert Weekdays from 'weekdays.csv'
--     with (ROWTERMINATOR = '0x0a',
--     DATA_SOURCE = 'cse414blob', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
-- FIRSTROW=1,TABLOCK);
--
-- -- Import for the large Flights table
-- -- This last import might take a little under 5 minutes on the provided server settings
--
-- bulk insert Flights from 'flights-small.csv'
--     with (ROWTERMINATOR = '0x0a',
--     DATA_SOURCE = 'cse414blob', FORMAT='CSV', CODEPAGE = 65001, --UTF-8 encoding
-- FIRSTROW=1,TABLOCK);

-- DELETE FROM [dbo].[WEEKDAYS] WHERE did = 9;
