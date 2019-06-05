Instructions for rolling over to new indices. Indices created: runindex, boxinfo, reshistory, hltdlogs and merging
1. 
a) Use from current location if no adjustments to mapping or index settings are needed
b) OR copy this directory (installed wit RPM as /opt/fff/tools/mapper) to another location (until fffmeta RPM provides updates)

In case of 1b):
 - mappings.py is a copy from mappings file from hltd git repository (either from BU/FU machines or hltd gitlab master branch) and can be updated by fetching a new version if necessary. Up to date version should always be used in case of doubt.
 - updatemappings.py script and river.jar can also be used from other location if necessary (default is /opt/fff). Look at makeIndices.sh script to redefine paths
2. run makeIndices.sh and specify subsystem. A new set of indices will be created with default settings and mapping, but not yet active until [3]. The script will suggest to follow up with other command. Specify second command line parameter to ENABLE ingest setting wich stores server side timestamp!
3. After making sure no errors appeared previously, run the suggested update alias command, or save it for later (it can change as it depends on date!). This should NOT be executed during an ongoing run!
4. makeLustreIndex.sh can be run if necessary. See /opt/fff/setup_river_index.py to create river index

