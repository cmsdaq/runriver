Mapping update setup script

run "make.sh" to create indices with initial settings.
The script will suggest to follow up with several other commands. For this, other dependencies are needed.

- fetch python/updatemappings.py and python/mappings.py from hltd repository (master branch)
- compile/get latest river-runriver jar package (from branch related to elasticsearch version).
Note:
- river 1.4.X (1.4.6 or higher only) is compatible with elasticsearch 2.2.0
- river 1.5.X is compatible elasticsearch 2.4.2
- river 1.6.X is compatible elasticsearch 5.1.1
Minor version difference might still work.
Otherwise change version or main elastic package and API dependency in pom.xml and build the jar

Indices created: runindex, boxinfo, hltdlogs and merging
Additional steps do the following
1. fill runindex with mappings contained in river repository
2. fill runindex,boxinfo and hltd with mappings contained in hltd repository
3. create run document in runindex (after which no more parent relations pointing to "run" can be defined)
4. copy runindex mappings into merging index mappings
5. switch aliases to point to newly initialized set of indices
