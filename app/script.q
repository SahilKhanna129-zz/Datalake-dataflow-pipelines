DROP TABLE IF EXISTS flight_data;
create external table flight_data (
Flight_date Date, Airline_ID Int, Carrier STRING) 
row format 
delimited fields terminated BY ',' 
LOCATION '${INPUT}';
