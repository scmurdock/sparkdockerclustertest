Submitting a spark application to a cluster using Docker:

To Install Docker See: https://docs.docker.com/get-docker/

Clone this repository to your computer using this command:

$ git clone https://github.com/scmurdock/sparkdockerclustertest.git

Change directories to the cloned repository:

$ cd sparkdockerclustertest

Start the Spark master and worker containers:

$ docker-compose up

After you type this command, you will see something similar to the following output:

spark_1           | 20/08/19 17:21:09 INFO Master: Registering worker 172.20.0.2:37815 with 1 cores, 1024.0 MiB RAM
spark-worker-2_1  | 20/08/19 17:21:09 INFO Worker: Successfully registered with master spark://e055383e2fee:7077
spark_1           | 20/08/19 17:21:09 INFO Master: Registering worker 172.20.0.3:33795 with 1 cores, 1024.0 MiB RAM
spark-worker-1_1  | 20/08/19 17:21:09 INFO Worker: Successfully registered with master spark://e055383e2fee:7077

You now have spark containers running (master, and two workers). 

Open another terminal then shell into the master node:

$ docker exec -it sparkdockerclustertest_spark_1 /bin/sh

Next submit the sparkpytest.py script to the master node:

$ $SPARK_HOME/bin/spark-submit --master spark://spark:7077 /home/workspace/sparkdockerclustertest/sparkpytest.py

You should see a message like this in the first terminal window (running the spark cluster):

spark_1           | 20/08/19 18:20:39 INFO Master: Registering app SimpleApp
spark_1           | 20/08/19 18:20:39 INFO Master: Registered app SimpleApp with ID app-20200819182039-0000

You should see a message like this in the window you used to submit the application:

Lines with a: 4, lines with b: 0

You should see a lot of console output, including a line similar to the following:

20/08/17 09:41:58 INFO DAGScheduler: Job 1 finished: count at NativeMethodAccessorImpl.java:0, took 0.059077 s
Lines with a: 4, lines with b: 0

This means the application finished running, and has produced the desired output



Submitting a Spark application to the Spark cluster using the project workspace:

https://github.com/scmurdock/sparkpyclustertest.git