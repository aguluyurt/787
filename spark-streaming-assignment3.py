from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with four working threads and a batch interval of 20 seconds
sc = SparkContext("local[4]", "NetworkWordCount")
ssc = StreamingContext(sc, 20)

# Read from the port:3333
line = ssc.socketTextStream("sandbox-hdp.hortonworks.com", 3333)

# Split each line into key and value
sensorValue = line.map(lambda r: r.split(" ")).collect()

#create rdd
rdd = sc.parallelize(sensorValue)

#calculate average, minimum and maximum
avg = rdd.groupByKey().mapValues(lambda x: sum(x) / len(x)).collect()
minimum = rdd.groupByKey().mapValues(lambda x: min(x))
maximum = rdd.groupByKey().flatMapValues(lambda x: max(x))

#print the calculated values
print("For (Sensor, Min)", "{}".format(minimum))
print("For (Sensor, Max)", "{}".format(maximum))
print("For (Sensor, Average)", "{}".format(avg))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
