--Query all records and return as JSON
SELECT 
    col
FROM 
    TABLE(sec_udtf_sql_api_oauth('SELECT * FROM DATA.TPCH_SF1.lineitem '  ));

--Query all records and return as Object
SELECT 
    to_object(col)
FROM 
    TABLE(sec_udtf_sql_api_oauth('SELECT * FROM DATA.TPCH_SF1.lineitem '  ));

--Query all records and return a AVG as number and name the column AVG_L_EXTENDEDPRICE
SELECT 
    to_object(col):AVG_L_EXTENDEDPRICE::number as AVG_L_EXTENDEDPRICE
FROM 
    TABLE(sec_udtf_sql_api_oauth('SELECT AVG(L_EXTENDEDPRICE) as AVG_L_EXTENDEDPRICE FROM DATA.TPCH_SF1.lineitem '  ));
