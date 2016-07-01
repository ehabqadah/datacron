#### datacron

Setting up and running the Spark files importer app on docker:
> install git, docker, docker compose, maven, java 8
> clone datacron project
> clone spark, cassandra, hadoop images as described in compose file
> cd datacron and execute:
  - docker-compose up cassandra1
  - docker-compose up hadoop-namenode
  - docker-compose up hadoop-datanode1
  - docker-compose up hadoop-secondarynamenode
  - docker-compose up master
  - docker-compose up worker
> Upload sample files to hdfs:
  - docker-compose exec hadoop-namenode /bin/bash
  - hdfs dfs -put /data/dfs/name/samples /samples
  - hdfs dfs -ls /samples
> Run Spark app:
  - docker-compose run app /bin/bash
  - /usr/spark/bin/spark-submit 
     --class de.fhg.iais.spark.app.main.Main 
     --master spark://master:7077  
     /files-importer/target/files-importer-0.0.1-SNAPSHOT.jar 
     -c /files-importer/conf/properties 
     -i hdfs://hdfs-namenode:9000/samples
> Check result:
   - docker-compose exec cassandra1 /bin/bash
   - cqlsh cassandra1
   - use keyspace example
   - select * from items ;

