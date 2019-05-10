#!/bin/env python
import sys
import xml.etree.ElementTree as EM
tree = EM.parse(sys.argv[1])
root = tree.getroot()

for child in root:
  if child.tag=="{http://maven.apache.org/POM/4.0.0}version":
    print(child.text)
    break

