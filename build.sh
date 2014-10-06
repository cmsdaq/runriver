#!/bin/bash
set -e

#echo 'DELETE RIVER'
#curl -XDELETE http://cu-01:9200/_river/runriver*

#echo 'CLEANING'
#mvn3 clean

echo 'COMPILING'
mvn3 package

#echo 'REMOVE PLUGIN'
#sudo /usr/share/elasticsearch/bin/plugin  -r river-runriver
#sudo rm /usr/share/elasticsearch/plugins/river-runriver/*

#echo 'INSTALL PLUGIN'
#sudo /usr/share/elasticsearch/bin/plugin  -url file:/home/salvo/Scrivania/PH-CMD/runRiver/target/releases/river-runriver-1.0-plugin.zip -i river-runriver
#sudo unzip /home/salvo/Scrivania/PH-CMD/runRiver/target/releases/river-runriver-1.0-plugin.zip -d /usr/share/elasticsearch/plugins/river-runriver/

#echo 'Restart ES'
#sudo service elasticsearch restart

#sleep 10
#echo 'CREATE RIVER'
#curl -XPUT http://cu-01:9200/_river/runriver/_meta -d '{    "type" : "runriver", "es_tribe_host" : "http://bu-01:9200" }' 

echo 'DONE'