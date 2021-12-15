# CSV SQL Query
A friendly query csv file use sql syntax

Usage:
```
$sql_csv.exe c:\temp\db.csv  c:\temp\author.csv
read csv file c:\temp\db.csv to table db
read csv file c:\temp\author.csv to table author
>select * from db;
Result:
+----+-----------+------+---------+
| id | name      | size | sport   |
+----+-----------+------+---------+
| 1  | Xiaoputao | 3    | Hiking  |
| 2  | Zgu       | 3    | Running |
| 3  | Xiaopang  | 2    | Walking |
+----+-----------+------+---------+
>
```
