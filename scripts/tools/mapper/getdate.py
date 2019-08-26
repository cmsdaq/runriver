#!/bin/env python3.4
from __future__ import print_function 
import datetime
now = datetime.datetime.now()
print(str(now.year)+str(now.month).zfill(2)+str(now.day).zfill(2))
