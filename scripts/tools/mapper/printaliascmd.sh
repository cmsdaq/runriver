#!/bin/bash

#uncomment to select subsystem and a new date

subsystem="cdaq"
newdate="20181127"
#used to add year alias (e.g. runindex_cdaq2017_read)
year=${newdate:0:4}   ## or put explicit year if newdate doesn't start with year

#subsystem="minidaq"
#newdate="20170704"


#make indices
echo
echo "#add set up aliases:"
echo ./updatemappings.py es-cdaq aliases ${subsystem} runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate} merging_${subsystem}_${newdate} $year

#TODO: add year alias mapping (like runindex_minidaq2017_read)
