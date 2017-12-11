#!/bin/bash
sudo yum -y install git
git clone https://github.com/bnajlis/parallel-bloom-filters.git /home/hadoop/parallel-bloom-filters
spark-submit --master yarn --deploy-mode cluster --conf "spark.dynamicAllocation.initialExecutors=5" /home/hadoop/parallel-bloom-filters/parallel-bloom-filter.py -mode create -data s3://bloom-filter-data/dataset_0001.dat -index hdfs:///user/hadoop/bloom-filter-index.dat