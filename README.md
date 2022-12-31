# Apache Spark 

# What is it? 
It's parallel computation engine built to handle large scale distributed data processing. Modularizing operations and available to 5 programming languages (Scala, Java, Python, SQL, and R). The engine shifted away from Apache Hadoop blueprint that incorporated both storage, and compute, to solely being a computation engine. This enabled spark to rely on other resources for it's storage, and allowed it to read from on premise and cloud data sources. Apache Spark become the "Unified Engine for Big Data Processing."

# How does it do it? 
The Spark architecture has a set of components that make this distributed execution possible, that line of communication flows the the Spark Driver, and SparkSession. 

* Spark Driver
  * The driver is our quarterback, accessing resources from the cluster manager and communicating with the executors to perform tasks on the workers.
* SparkSession 
  * Grants access to the library of spark functionalities. 
 
# Why spark for Big Data? 
Spark uses the driver to distribute chunks of data to different executors, a process that is known as partitioning. This distribution of data is what makes it possible to achieve parallelism. You could think of it as an offensive line, you will never see one lineman blocking the entire defensive front, or a lineman leaving his post to run a route (although they are doing that nowadays). What they do take care of the assigment closest to them. The work is distributed amongst the group in order to remain highly efficient. The proper configuration for a workload requires understanding the size, and velocity for a given task will have to handle throughout it's lifecycle. This repo is simply displaying different functionalities accessible in a SparkSession, so configuration will be held up for a different time. 

# Data Engineering Experience 
Earlier this year I was given a wonderful opportunity to work with Apache Spark to help build, and be apart of a migration (moving from one system to another). It was quite a change from working solely with the pandas dataframes and small csv, and excel files, but I always welcome the challenge. I think working through challenges promotes the best growth. It has been such a great experience, and I feel like I have only scratched the surface in this brief introduction to the Data Engineering World. 

I wanted to have some fun and use open source data sets provided in the "Our World in Data" Repo [here](https://github.com/owid/owid-datasets/tree/master/datasets) to use the functionalities enabled from the SparkSession. Although I wont be use an EC2 instance (unless someone wants to pay for it) I will still be able to read, transform, and write our data. 

# I will update the step by step process in a different commit here. 
