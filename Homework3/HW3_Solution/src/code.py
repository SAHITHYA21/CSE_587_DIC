from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import time

# Creating k-mers of length 3
def create_kmers(text):
    k = 3
    return list(map(lambda i:text[i:i+k], range(len(text)- k+1)))

#Function to process each RDD and count the frequency of k-mers
def process_kmers(rdd):
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"\n\nProcessing batch at: {current_time}")
    
    # Generating k-mers of length 3 for each line in the RDD and count k-mers
    kmer_list = rdd.flatMap(lambda line:create_kmers(line))
    kmer_count= kmer_list.map(lambda kmer:(kmer,1)).reduceByKey(lambda x,y:x+y)
    #kmer_count.pprint()

    
    # To print the k-mers for each line (k=3)
    #for line in rdd.collect():
        #line_kmers=create_kmers(line)
        #print(f"K-mers in line: '{line}': {' '.join(line_kmers)}")

    # Sorting the k-mers by frequency and taking the top 10 k-mers
    top_kmers = kmer_count.takeOrdered(10, key=lambda x: -x[1])

    print("\nTop 10 K-mer counts:")
    for kmer, freq in top_kmers:
        print(f"Count of '{kmer}': {freq}")

    # Saving all kmer counts in output_kmer_count.txt file
    with open("output_kmer_count.txt", "w", encoding="utf-8") as count_file:
        for kmer,count in kmer_count.collect():
            count_file.write(f"{kmer}: {count}\n")

    #Saving all kmer counts in output_kmers.txt file
    with open("output_kmers.txt", "w", encoding="utf-8") as kmers_file:
        for line in rdd.collect():
            line_kmers=create_kmers(line)
            kmers_file.write(f"K-mers for line '{line}': {' '.join(line_kmers)}\n")


# Creating Spark context with 2 execution threads and naming App as "KMerCountApp"
conf=SparkConf().setAppName("KMerCountApp").setMaster("local[2]")
sc=SparkContext(conf=conf)
# Setting 10 second batch interval
stream_con=StreamingContext(sc,10)

# DStream for recieving data from localhost (port 9999)
text_lines=stream_con.socketTextStream("localhost",9999)
# Applying a window of 30 seconds with a sliding interval of 10 seconds
windowed_lines = text_lines.window(30, 10)
# Applying the k-mer processing function to each RDD in the windowed DStream
windowed_lines.foreachRDD(process_kmers)

# Start the streaming
stream_con.start()
stream_con.awaitTermination()
