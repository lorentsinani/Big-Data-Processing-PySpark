# RDD 

## Examples for class

### Example 1: Map and Collect

Use map() transformation to cube each number of the “numbRDD” RDD that you can create from a list of numbers. Next, you'll store all the elements in a variable and finally print the output.

### Example 2: Filter and Count

Filter out lines containing keyword Spark from “fileRDD” RDD which consists of lines of text from the “examplefile.md” file. Next, you'll count the total number of lines containing the keyword Spark and finally print the first 4 lines of the filtered RDD.

### Example 3: ReduceByKey and Collect

First create a pair RDD from a list of tuples, then combine the values with the same key and finally print out the result. Instructions:
Create a pair RDD named Rdd with tuples (1,2),(3,4),(3,6),(4,5).
Transform the Rdd with reduceByKey() into a pair RDD Rdd_Reduced by adding the values with the same key.
Collect the contents of pair RDD Rdd_Reduced and iterate to print the output.

### Example 4: SortByKey and Collect

Sort the pair RDD “Rdd_Reduced” that you created in the previous exercise into descending order and print the final output.
Instructions:
Sort the Rdd_Reduced RDD using the key in descending order.
Collect the contents and iterate to print the output.

### Example 5: Count by keys

Create and use a Rdd and count the number of unique keys in that pair RDD.
Instructions:
countByKey and assign the result to a variable total.
What is the type of total?
Iterate over the total and print the keys and their counts.


