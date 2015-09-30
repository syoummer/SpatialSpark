# Big Spatial Data Processing using Spark

[![Join the chat at https://gitter.im/syoummer/SpatialSpark](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/syoummer/SpatialSpark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/syoummer/SpatialSpark.svg?branch=master)](https://travis-ci.org/syoummer/SpatialSpark)

## Introduction

[SpatialSpark](http://simin.me/projects/spatialspark/) aims to provide efficient spatial operations using Apache Spark. It can be used as a Spark library for
spatial extension as well as a standalone application to process large scale spatial join operations.  

## Install

To compile SpatialSpark, [SBT](http://www.scala-sbt.org/) is required. After installed and configured SBT, simply run
    
    sbt assembly

SpatialSpark has been compiled and tested on Spark 1.3.0. The version can be changed in
`build.sbt` file. For geometry operations, well known [JTS](http://www.vividsolutions.com/jts/JTSHome.htm) library is used.
The JTS library version in SpatialSpark is 1.13.

## Usage 

### Library
Import the jar file or copy the source files directly to your own project. 

### Standalone Application
Use `spark-submit` submit a Spark job.

#### Spatial Join
Run spatial join processing on two datasets with spatial information (in 
[WKT](http://en.wikipedia.org/wiki/Well-known_text) format) Use `--help` to list all available options.

*Example*  
Assuming two input datasets (A and B) have been uploaded to HDFS (or S3 for Amazon AWS), below is a broadcast based
spatial join example.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    SpatialSpark-assembly-1.0.jar --left A --geom_left 0 --right B --geom_right 0 --broadcast true --output output \
    --partition 1024 --predicate within 
    
If both datasets are very large, so that the right dataset cannot fit in memory, here is an example of performing
partition based spatial join.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    SpatialSpark-assembly-1.0.jar --left A --geom_left 0 --right B --geom_right 1 --broadcast false --output output \
    --partition 1024 --predicate within --method stp --conf 32:32:0.1 --parallel_part true

We have provided two sample datasets, including one point dataset (`data/point1k.tsv`) and one polygon
dataset (`data/nycb.tsv`).

For broadcast based spatial join, use

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    SpatialSpark-assembly-1.0.jar --left data/point1k.tsv --geom_left 1 --right data/nycb.tsv --geom_right 0 \
    --broadcast true --output output --predicate within

For partition based spatial join with STP, use

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.SpatialJoinApp \
    SpatialSpark-assembly-1.0.jar --left data/point1k.tsv --geom_left 1 --right data/nycb.tsv --geom_right 0 \
    --broadcast false --output output --predicate within --method stp --conf 32:32:0.1 \
    --parallel_part false

#### Spatial Partition
Generate a spatial partition from input dataset, currently Fixed-Grid Partition (FGP), Binary-Split Partition (BSP) and
Sort-Tile Partition (STP) are supported. Use `--help` to list all options.

#### Spatial Range Query
Spatial range query includes both indexed and non-indexed query. For non-indexed query, a full scan is performed on the
dataset and returns filtered results. here is an example,

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Query \
    SpatialSpark-assembly-1.0.jar --input data/point1k.tsv --geom 1 --output output.tsv \
    --query 98500.0,181800.0,986000.0,182000.0

Since a full scan needs to load the whole dataset, the performance may be bad if the dataset is very large. To improve
the performance, an indexed range query is supported.

Before performing the indexed range query, an index need to be created. An example is shown below.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Index \
    SpatialSpark-assembly-1.0.jar --input data/point1k.tsv --geom 1 --output data/point1k_new \
    --conf 32:32:0.3

After the job, the dataset will be re-ordered and saved in the specified output location and an index file will
be created in the specified output location with `"_index"` suffix. The index file is separated with the data, and the
content of the new dataset is as same as the orignal one but in different order.

With created index, the range query can be performed very fast.

    bin/spark-submit --master spark://spark_cluster:7077 --class spatialspark.main.Query \
    SpatialSpark-assembly-1.0.jar --input data/point1k.tsv --geom 1 --output output.tsv \
    --query 98500.0,181800.0,986000.0,182000.0 --use_index true

## Future Work
- Add tests
- More documentations
- Spatial indexed range query using R-tree and Grid-file
- KNN search 


## Contact
If you have questions and comments, contact [me](http://simin.me).

 Copyright 2015 Simin You
  
 Licensed under the Apache License, Version 2.0 (the "License");  
 you may not use this file except in compliance with the License.  
 You may obtain a copy of the License at  
  
 http://www.apache.org/licenses/LICENSE-2.0  
  
 Unless required by applicable law or agreed to in writing, software  
 distributed under the License is distributed on an "AS IS" BASIS,  
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 See the License for the specific language governing permissions and  
 limitations under the License.  
     



 
