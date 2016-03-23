cd $1
echo installing elasticsearch plugin $3 ...
bin/plugin install file:///opt/fff/esplugins/$2 -s || true

