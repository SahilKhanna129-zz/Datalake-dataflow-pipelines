-- filter null values from the data
 
A = load '/user/root/raw_data/flight_data' using PigStorage(',');
B = filter A by $7 is not null and $8 is not null;
C = foreach B generate $5, $7, $8;
store C into '/user/root/processed_data/flight_data' USING PigStorage(',');
