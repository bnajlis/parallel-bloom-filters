#!/bin/bash
sudo yum -y install git
git clone https://github.com/bnajlis/parallel-bloom-filters.git /home/hadoop/parallel-bloom-filters
spark-submit --master yarn --deploy-mode cluster /home/hadoop/parallel-bloom-filters/parallel-bloom-filter.py -mode create -data s3://bloom-filter-data/dataset_0001.dat -index s3://bloom-filter-data/bloom-filter-index