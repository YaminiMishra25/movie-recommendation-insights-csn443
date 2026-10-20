#!/bin/bash
# Run on a real Hadoop cluster / pseudo-distributed node. Adjust the streaming jar path.
J=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
hdfs dfs -mkdir -p /movies/ratings /movies/movies && hdfs dfs -put -f data/raw/ratings.dat /movies/ratings/ && hdfs dfs -put -f data/raw/movies.dat /movies/movies/
for job in movie genre user genre_rating user_genre month; do
  hdfs dfs -rm -r -f /movies/out/$job
  hadoop jar $J -files mapreduce/mr_jobs.py,data/raw/movies.dat -input /movies/ratings -output /movies/out/$job \
    -mapper "python3 mr_jobs.py map $job" -combiner "python3 mr_jobs.py reduce" -reducer "python3 mr_jobs.py reduce"
done
