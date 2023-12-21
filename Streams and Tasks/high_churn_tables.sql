create database demodb;
create schema streams;
use schema demodb.streams;
create warehouse MYWH warehouse_size = SMALL auto_suspend=300;
use warehouse MYWH;

create or replace table source_table (
id int,
name varchar,
timestamp datetime
);
create or replace table target_table (
id int,
name varchar,
timestamp datetime
);


create or replace view source_view as select * from source_table;

select * from source_table;
select * from target_table;

insert into source_table values (1,'Daniel', current_timestamp() );
select * from source_table;

--Add the new record to the Target table
INSERT INTO target_table SELECT * FROM source_table;
select * from target_table;
select * from source_table;

--Create a Snowflake Stream for CDC
create or replace stream source_stream on view source_view append_only=true;
select * from source_stream;

--Misspelled Georg here and instead it says Gorg
insert into source_table values (2,'Gorg', current_timestamp());
select * from source_stream;
select * from source_table;

--Added a new record with Georg corectly spelled
insert into source_table values (2,'Georg', current_timestamp());
select * from source_table;
select * from source_stream;


--Create a view that only shows the latest record from the STREAM 
CREATE or replace view source_view_latest as select * from source_stream QUALIFY row_number() over (partition by ID order by timestamp ASC) = 1;
SELECT * FROM source_view_latest;

SELECT * FROM target_table;

--Create UNION VIEW to show the latest record from the STREAM  and from the Fact table
CREATE or replace view source_target_union as
WITH T AS(SELECT ID, NAME, TIMESTAMP  from target_TABLE
UNION ALL
SELECT ID, NAME, TIMESTAMP FROM  source_view_latest)
SELECT * FROM T
QUALIFY row_number() over (partition by ID order by timestamp desc) = 1 ORDER BY ID ASC;

--Show records from the new UNION view
SELECT * FROM  source_target_union;


--INSERT INTO target_table select ID, NAME, timestamp from source_view_latest;
SELECT * FROM source_table;
SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union;

--Create a stored procedure that merges records from the View on the Snowflake Stream to the data Mart
create procedure merge_records()
RETURNS VARCHAR
AS
$$
MERGE INTO target_table USING source_view_latest 
    ON target_table.id = source_view_latest.id
    WHEN MATCHED THEN 
        UPDATE  
            SET 
                target_table.name = source_view_latest.name,
                target_table.timestamp = source_view_latest.timestamp
    WHEN NOT MATCHED THEN
            INSERT (ID, NAME, timestamp) VALUES(ID, NAME, timestamp); 
$$;

--Run the merge operation as a stored procedure            
CALL merge_records();

SELECT * FROM source_table;
SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union; 

insert into source_table values (3,'Philip', current_timestamp());

SELECT * FROM source_table;
SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union;

CALL merge_records();

SELECT * FROM source_table;
SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union;

insert into source_table values (3,'Filip', current_timestamp());

SELECT * FROM source_table;
SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union;

CALL merge_records();

SELECT * FROM source_stream;
SELECT * FROM target_table;
SELECT * FROM source_target_union;
