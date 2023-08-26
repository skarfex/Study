copyto:
	scp ./*.py root@158.160.24.77:/tmp/mapreduce/ && scp ./*.sh root@158.160.24.77:/tmp/mapreduce/

copyfrom:
	scp root@158.160.24.77:/root/yellow_tripdata_2020-12.csv /home/skarfex/
    scp ubuntu@158.160.18.209:/home/ubuntu/result.csv /home/skarfex/

get_from_hdfs:
    hadoop fs -get /user/root/output-data /root/