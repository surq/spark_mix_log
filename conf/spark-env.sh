# Options read in YARN client mode
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
HADOOP_CONF_DIR=/home/dmp/app/hadoop-2.3.0-cdh5.0.0-och3.1.0/etc/hadoop
# - SPARK_EXECUTOR_INSTANCES, Number of workers to start (Default: 2)
SPARK_EXECUTOR_INSTANCES=2
# - SPARK_EXECUTOR_CORES, Number of cores for the workers (Default: 1).
SPARK_EXECUTOR_CORES=2
# - SPARK_EXECUTOR_MEMORY, Memory per Worker (e.g. 1000M, 2G) (Default: 1G)
SPARK_EXECUTOR_MEMORY=1G
# - SPARK_DRIVER_MEMORY, Memory for Master (e.g. 1000M, 2G) (Default: 512 Mb)
SPARK_DRIVER_MEMORY=1G
# - SPARK_YARN_APP_NAME, The name of your application (Default: Spark)
# - SPARK_YARN_QUEUE, The hadoop queue to use for allocation requests (Default: ‘default’)
# - SPARK_YARN_DIST_FILES, Comma separated list of files to be distributed with the job.
# - SPARK_YARN_DIST_ARCHIVES, Comma separated list of archives to be distributed with the job.

#MASTER=yarn-client
MASTER=local[2]
#SPARK_JAVA_OPTS+="-Dspark.local.dir=/data1/mix_spark/spark_log/tmp"