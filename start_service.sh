#!/bin/bash

sh ~/Desktop/elasticsearch/elasticsearch-6.5.0/bin/elasticsearch &
sleep 15
sh ~/Desktop/elasticsearch/kibana-6.5.0-darwin-x86_64/bin/kibana &
sleep 10
python gen_test_data.py -dcmi