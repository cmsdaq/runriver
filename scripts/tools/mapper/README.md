Instructions for rolling over to new indices. Indices created: runindex, boxinfo, hltdlogs and merging
1. Fetch latest mappings.py from hltd master branch. updatemappings.py (/opt/fff) might be also necessary if newer than provided is needed.
2. find, retrieve or comple latest river-runriver jar package, available in this repository (used in es-cdaq hosts in /opt/fff) and link to it as river.jar
3. run makeIndicesCdaq.sh (or equivalent for system concerned). A new set of indices will be created with default settings and mapping, but not yet active until [4]. The script will suggest to follow up with other command.
4. After making sure no errors appeared previously, run the suggested update alias command, or save it for later. This should NOT be executed during an ongoing run!
5. makeLustreIndex.sh can be run if necessary. See /opt/fff/setup_river_index.py to create river index

