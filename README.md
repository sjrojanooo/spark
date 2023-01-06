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

I wanted to have some fun and use open source data sets provided by Kaggle "Adidas Sales" to use the functionalities enabled from the SparkSession. 

# Docker 
# Build Docker Image 
The docker image will containerize the application and make readily available for you. 
To run this make sure you have Docker installed. It can be found [here](https://docs.docker.com/engine/install/). After installation you can build the image following the list of commands. Once you've built the images, you can open up the docker desktop to view the images that are in use. 

Basic Docker commands. 
1. To build and name the image
  * `docker build -t spark .`
2. Run the image in "detached mode", calling the name you just gave it in the build command. 
  * `docker run -d spark`
3. This will show the list of running images with an active status
  * `docker ps`
4. Will show a list of images running with any type of status
  * `docker ps -a`
5. Stops a container, you much provide the container id. 
  * `docker stop <containter_id>`
6. If you want to completely remove the image, to force remove you wil add and `-f` flag after the rmi. 
  * `docker rmi <image_id>`

If you want to view what made up the layer image you can run `docker image history spark`. Finally, to view the images that are in use, you can open up the Docker Destkop and view the images. 

![Image](https://github.com/sjrojanooo/spark/blob/main/images/in-use-images.png)

### What are we installing? 
* python
* spark
* correct version of java sdk
* packages in the requirments.txt file
  * findspark
    * Spark might not be download on you sys.path, so findspark locates spark, to make it accessible in your IDE. 
  * pytest
    * This package is used for our unit test. This will promote cleaner code and will also verify we are getting the expected output from our functions/methods.

# Unit Test
Unit test promote better code and provide an assertion on the expcted outcome for a given transformation. I am guilty of simply just writing code and trying to do much in a single functions. This has truly helped me create reusable / readable functions. It gives you a chance to create your own data and really understand the transformation that is taking place. Pytest Documenation can be found [here](https://docs.pytest.org/en/7.2.x/). You can find the test units inside of the `test/test_adidas_transformations.py`. We provide pytest configurations  inside of the `conftest.py` file, this allows us to provide a global scopr session that can be used throughout all of our test units. The obvious session that all of our units will need will be our SparkSession, since it's the library we we will be using to access all of sparks built in functionalities. 

To run the unit test you can execute it using the `docker-compose up test`

### What is this program doing? 
Well I manually downloaded this dataset from the Kaggle website, which came already zipped inside of an archive folder. I am using pythons `ZipFile` package to extract the adidas retail sales file without unzipping the object. We then use some basic `os` and `shutil` functionalities to create a separate directory and rename the file to a more readable format. Next we use our `SparkSession` to read the file and commit some simple transformations. 

1. Raname all columns to a more standardized format. 
2. Transform colum values that have literal values such as `($, %, and ",")`
3. Transform datetime values from `(1/2/20 -> 2020/01/02)` formats so that we can use some date transformation funcitonalities. 
4. Simple aggregations to identifies when adidas started selling their merchandise at a given store
5. Sum all aggregated values by region using a window function 
6. Find the percentage of each aggregated value that each store takes up in a given region. 
7. write out the parquet file to a given location. 
8. Why parquet? Well unlike csv's parquet file are much more powerful, having the ability to store data type for a given column. Instead of having to read the a file and commit the transformations explained above again, the data will already come prepared as is. When you get to handling bigger data, the partitioning of a file will come to play as well. 
