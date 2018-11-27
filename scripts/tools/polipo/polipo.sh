#!/bin/sh

#this script sets up sock proxy for node.js component installation within private network using NPM

#prerequisite: build polipo (proxy software) source
#https://www.irif.fr/~jch/software/polipo/

echo "after typing your password, do ctrl+Z, execute 'bg'"
ssh -N -D 2056 cmsusr #-N? or do it in other shell

echo "starting polipo in the background..."
polipo/polipo socksParentProxy=localhost:2056 &
#test: curl --proxy localhost:8123 https://www.google.com

#environment
#export PATH=/cmsnfses-web/es-web/scratch/node/node-v6.8.1-linux-x64/bin/:${PATH}
mkdir /tmp/temp-home-es-cdaq
export HOME=/tmp/temp-home-es-cdaq

#proxy settings in npm
npm config set proxy http://localhost:8123
npm config set http-proxy http://localhost:8123
npm config set https-proxy https://localhost:8123

#maybe if https doesn't work:
#npm config set strict-ssl false
#npm config set registry "http://registry.npmjs.org/"


#make installation:

npm install #...

#when finished:
npm config rm https-proxy
npm config rm http-proxy
npm config rm proxy

echo "use control+C twice (to kill polipo and ssh client)"
fg
fg

