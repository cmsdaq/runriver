cd $1
echo installing elasticsearch plugin $3 ...
bin/elasticsearch-plugin install file:///opt/fff/esplugins/$2 -s || true

