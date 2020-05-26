# Workshop-ApacheSpark

## Team Members 

### 1.  Akhitha Tumula
<img src="GroupMembers/akhitha-picture.jpeg" alt="drawing" width="100"/>

### 2. SatishKumar Mandapalli
<img src="GroupMembers/satishkumar_Photo.jpg" alt="drawing" width="100"/>

### 3. Sai Chandu Gampa
<img src="GroupMembers/chandu.jpg" alt="drawing" width="100"/>

## Profile Links
- [Akhitha Tumula](https://github.com/thumula-akhitha)
- [Satishkumar Mandapalli](https://github.com/mandapallisatish64)
- [Sai Chandu Gampa](https://github.com/saichandugampa)

## Introduction
**Apache Spark** is an open source cluster computing platform for real-time processing. This is one of the most popular projects of the Apache Software Foundation. Spark has clearly emerged as the industry leader in the production of Big Data.Today, Spark is being embraced by top names including Amazon , eBay, and Yahoo! Several companies are running Spark on clusters of thousands of nodes.It has a thriving open-source community and is the most active Apache project at the moment. Spark provides an interface for programming entire clusters with implicit data parallelism and fault-tolerance.
- It was built on top of Hadoop MapReduce and it extends the MapReduce model to efficiently use more types of computations.
### Features of Apache Spark
1. **Polyglot**:Spark offers high-level APIs for Java, Scala, Python and R. The Spark code can be written in any of these four languages. It's providing a shell for Scala and Python. The Scala shell can be accessed from the installed directory via ```./bin / spark-shell``` and Python shell via ```./bin / pyspark```.
2. **Speed**:Spark is up to 100 times faster than Hadoop MapReduce for large-scale data processing. Spark is able to achieve this speed by means of controlled partitioning. It manages data using partitions that help to parallel distributed data processing with minimal network traffic.
3. **Multiple Formats**:Spark supports various data types, such as Parquet, JSON, Hive and Cassandra, in addition to the standard formats such as text files, CSV and RDBMS tables. The Data Source API provides a pluggable mechanism for accessing structured data through Spark SQL. Data sources can be more than just simple pipes that convert data and convert it to Spark.
4. **Lazy Evaluation**:Apache Spark is delaying its evaluation until it is absolutely necessary. This is one of the main factors that contributes to its size. For transformations, Spark adds them to the DAG (Directed Acyclic Graph) of the computation and only when the driver requests any data does the DAG actually get executed.
5. **Real Time Computation**:Spark's computation is real-time and has low latency due to its in-memory computation. Spark is designed for massive scalability and the Spark team has recorded users of the framework operating production clusters with thousands of nodes and assisted a variety of computational models.


We are going to discuss Apache spark with three different programming languages Scala, Java and Python.

## Scala
Scala is a modern and multi-paradigm programming language. It has been designed for expressing general programming patterns in an elegant, precise, and type-safe way. One of the prime features is that it integrates the features of both object-oriented and functional languages smoothly.

#### Prerequisites
- To install spark click on this link 
[Download Spark](https://spark.apache.org/downloads.html)
- Download the spark file and unzip it using this command ```tar xzvf spark-2.4.5-bin-hadoop2.7.tgz```
- You will get the folder in windows C and set the enivronment variables in system variables.

### Basic commands in scala
Open powershell from anywhere and run the command spark-shell it automatically opens the shell.
1. This reads the content from the file
```val value = spark.read.textFile("input.txt")```
2. This displays how many lines are in the textFile
```value.count()```
3. This displays the first line in the textFile
```value.first()```
4. This displays the output that in how many lines "s" letter is present.
```value.filter(line => line.contains("s")).count()```

<img src="scala/Screenshots/basic-commands.png" alt="drawing" width="600"/>

### To find the Most Repeated Word count in a file using scala

1. This reads the information from the input.txt
```val fileName = sc.textFile("input.txt")```
2. It splits the lines with spaces and displays the words which are more than 0
```val wordLength = fileName.flatMap(line => line.spilt(" ")).filter(word => word.trim().length() > 0)```
3. This displays the output of the wordLength
```wordLength.toDF().show()```
4. It appends the value 1 and it compares the key and value if the key is repeated then value is increased.
```val reduceWord = wordLength.map(word=>(word,1)).reduceByKey((a,b) => a + b)```
5. This displays the output of the reduceWord
```reduceWord.toDF().show()```
6. This displays most repeated word in the file and its count.
```val mostRepeatedWord = reduceWord.reduce((a,b) => if (a._2 > b._2) a else b)```

<img src="scala/Screenshots/wordcount-1.png" alt="drawing" width="600"/>

<img src="scala/Screenshots/wordcount-2.png" alt="drawing" width="600"/>

7. Spark shell jobs:

<img src="scala/Screenshots/web-interface.png" alt="drawing" width="600"/>


## Python
Spark SQL brings native support for SQL to Spark and streamlines the process of querying data stored both in RDDs (Spark’s distributed datasets) and in external sources. To execute SQL queries we are doing with python. 

#### Prerequisties
- Install spark and scala before downloading python
- Download anaconda python click on this link to follow the instructions
 [https://docs.anaconda.com/anaconda/install/windows/](https://docs.anaconda.com/anaconda/install/windows/)
- set the environment variables in system variables of python.

 
### SQL Queries peformed on JSON file






### SQL Queries performed on CSV file




## Java
Even though Scala is the native and more popular Spark language, many enterprise-level projects are written in Java and so it is supported by the Spark stack with it’s own API.
### Steps to work with java:
1. Download eclipse from https://www.eclipse.org/downloads/
2. Open and create the Maven project
3. Add jar files which was present in Spark folder to the maven project jar folder.
4. Update POM.xml file with spark version











## References
1. https://spark.apache.org/docs/latest/quick-start.html
