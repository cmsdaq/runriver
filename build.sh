#!/bin/bash
rm target/*.jar -rf
mvn clean compile assembly:single -e
#set -e

