Our top level interpreter/harness that reads the input and produces the output is called "Harness.java"
Logic of extracting join conditions:
First deal with other AND conditions(if there is any) in where clause. In this way, it is able to save time for not computing tuples who do not satisfy AND conditions.
Then, 'operators/JoinOperator.java' processes incoming left Tuple with left schema, right Tuple with right Schema. Using Nested Join Loop algorithms to formulate tuples that satisfy conditions. Whenever inner loop gets null, reset inner loop to the begining.
Our interpreter program takes in 'input directory' as 'cs4321/project1/samples/input' and processes 'input directory' in 'util/DBCatalog.java'. If program having any trouble in finding correct path, please let us know.
Please note, this program runs in Java 8.
