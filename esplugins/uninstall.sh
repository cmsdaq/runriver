#!/bin/bash
cd $1
echo uninstalling elastic plugin $2 ...
bin/plugin remove $2 -s || true

