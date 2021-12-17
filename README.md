# CSV SQL Query
A friendly query csv file use sql syntax

## Installation
```
$ cargo install sql_csv
```
## Support Commands
**load**

Load csv file 
```
load /home/path/to/xxx.csv 
```
**store**

Store last success SQL query result to csv file 
```
store /path/to/xxx.csv
```
**SQL**

All SQL query support.

Usage:
```
$sql_csv.exe c:\temp\user.csv  c:\temp\author.csv
read csv file c:\temp\user.csv to table user
read csv file c:\temp\author.csv to table author
>select * from user;
Result:
+----+-----------+------+---------+
| id | name      | size | sport   |
+----+-----------+------+---------+
| 1  | Xiaoputao | 3    | Hiking  |
| 2  | Zgu       | 3    | Running |
| 3  | Xiaopang  | 2    | Walking |
+----+-----------+------+---------+
>
>load c:\temp\abc.csv
load csv file c:\temp\abc.csv to table abc
Load ok.
>select * from abc;
Result:
+----+-----------+------+---------+
| id | name      | size | sport   |
+----+-----------+------+---------+
| 1  | Xiaoputao | 3    | Hiking  |
| 2  | Zgu       | 3    | Running |
| 3  | Xiaopang  | 2    | Walking |
+----+-----------+------+---------+
>store c:\temp\bar.csv
Store ok.
>
```
