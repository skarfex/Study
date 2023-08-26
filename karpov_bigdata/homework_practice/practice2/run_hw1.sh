export MR_OUTPUT=/user/root/output-data

hadoop fs -rm -r $MR_OUTPUT


hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Taxi month report streaming job' \
-Dmapred.reduce.tasks=1 \
-Dmapreduce.map.memory.mb=1024 \
-file /tmp/mapreduce/mapper_hw1.py -mapper /tmp/mapreduce/mapper_hw1.py \
-file /tmp/mapreduce/reducer_hw1.py -reducer /tmp/mapreduce/reducer_hw1.py \
-input /user/root/2020  -output $MR_OUTPUT

#hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
#-Dmapred.job.name='Simple streaming job reduce' \
#-Dmapred.reduce.tasks=1 \
#-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
#-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
#-input /user/root/gzip-data -output $MR_OUTPUT

# -Dmapred.reduce.tasks=1 \
#-Dmapreduce.input.lineinputformat.linespermap=1000 \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
#-input /user/root/input-data -output $MR_OUTPUT


## s3
#-Dfs.s3a.endpoint=s3.amazonaws.com -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
#-Dmapreduce.map.memory.mb=1024 \

#hadoop fs -rm -r taxi-output

