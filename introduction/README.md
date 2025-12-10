# Install pyspark using PIP
`pip install pyspark`

Run pyspark:

`pyspark`

This will open pyspark shell which can be used to express logic directly line by line.

## Example 1 using Pyspark shell

```
numb = range(1,10)
spark_data = sc.parallelize(numb)

# To see the `spark_data` we should collect its value by:
spark_data.collect()
```

We can try some other actions like map, filter and sum

```
spark_data.map(lambda x: x*2).collect()

spark_data.filter(lambda x: x%2).collect()

spark_data.sum()
```

To exit we run:
`exit()` or press **ctrl+d**

## Run example 1 as python script using PySpark

1. Create the file i.eg `example1.py`
```
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example1") \
    .getOrCreate()

sc = spark.sparkContext

# example1 code logic

# good practice to stop spark
spark.stop()
```
2. Run it with system python
`python example1.py` or `python3 example1.py`
