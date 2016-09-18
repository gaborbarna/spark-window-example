# spark-window-example

## Prerequisites

[sbt][] 0.13.x

[sbt]: http://www.scala-sbt.org/

## Running

    sbt "run <path-to-input>"

Or

    sbt assembly
    java -jar target/scala-2.11/spark-window-example-assembly-0.0.1.jar <path-to-input>

Alternatively you can submit the uberjar to your Spark cluster.


## Running tests

    sbt test
