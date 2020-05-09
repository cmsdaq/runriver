#!/bin/env python3.4
from __future__ import print_function
import sys
sys.path.append('/opt/fff')
from insertRiver import parse_esweb_pwd
import json
print(parse_esweb_pwd(sys.argv[1]))
