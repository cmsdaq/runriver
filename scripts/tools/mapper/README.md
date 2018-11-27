Instructions for rolling over to new indices. Indices created: runindex, boxinfo, hltdlogs and merging
1. Fetch latest mappings.py and updatemappings.py from hltd git master branch (python/)
2. retrieve or comple latest river-runriver jar package, available in this repository (used in es-cdaq hosts) and link it to the river.jar name
3. run makeIndicesCdaq.sh (or equivalent for system being rolled). A new set of indices will be created with default settings and mapping, but not yet active until [4]. The script will suggest to follow up with other command.
4. After making sure no errors appeared previously, run the suggested update alias command, or save it for later. This should NOT be executed during an ongoing run!

