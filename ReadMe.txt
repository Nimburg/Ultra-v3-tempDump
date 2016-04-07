3rd re-write in Python and MySQL of Project Ultra. 

1. written with Python 2.7
2. with standard libraries numpy, scipy, matplot, json, pandas
3. used pymysql for MySQL application
4. used sklearn for k-means clustering

Please Note: 

1. Although luigid code was provided, however, Stage3 was NOT written with this in mind.
One should adjust the SQL commands of Stage3 for full luigid operation. (will update this part soon.)
2. One should create necessary data bases beforehand, and update parameters into the code accoordingly. 
3. Stage4 is the k-means clustering part. Recommend excecuting it independently. Stage4 is more of a check (and viewer) of previous stages' result; rather than any serious attemp of achieving certain conclusions. 

Please Note: 

The current state of this 3rd re-write of project ultra is, once again, in dead end. The reasons include:

1. data cleaning speed is very low. Even with only "1-dimensional projection", data cleaning speed (stage2) is at least 5 times SLOWER than real time speed. 
2. Stage2 need modification to allow for "multi-dimensional projection" for calculating rolling distances. 
3. given the question under study, simply increasing dimension of data is not necessarily the correct approach. 
