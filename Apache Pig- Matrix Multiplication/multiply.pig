
 
first_matrix = LOAD '$M' USING PigStorage(',') AS (row, col, value);
--sh echo $M

second_matrix = LOAD '$N' USING PigStorage(',') AS (row, col, value);
--sh echo $N


join_matrix = JOIN first_matrix BY col, second_matrix BY row;


 
resulting_matrix = FOREACH join_matrix GENERATE $0 AS row ,$4 AS col ,$2*$5 AS value ;

 
matrix_after_grouping = GROUP resulting_matrix  BY (row, col);
 
final_result = FOREACH matrix_after_grouping GENERATE group.$0 as row , group.$1 as col, SUM(resulting_matrix.value) AS val ;

 
DUMP final_result;
STORE final_result INTO '$O' USING PigStorage(',');
