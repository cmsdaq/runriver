#!/bin/bash -e
echo -e '\n\n'
echo -e '\nDELETE OLD ZIP FILES\n'
sshpass -p 'ominozzo2' parallel-ssh -h /home/salvo/Scrivania/PH-CMD/cdaCloud/cuhosts -l bufu -AIi "rm /home/bufu/river-runriver-*.zip" < /home/salvo/Scrivania/PH-CMD/cdaCloud/pass

echo -e '\nUPLOADING\n' #set VERSION!!!!!!!!!!!!!!!!!!!
sshpass -p 'ominozzo2' parallel-scp -h /home/salvo/Scrivania/PH-CMD/cdaCloud/cuhosts -l bufu -A /home/salvo/Scrivania/PH-CMD/f3web/runriver/target/releases/river-runriver-1.3.3-plugin.zip  /home/bufu

echo -e '\nUNINSTALL OLD PLUGIN\n'
sshpass -p 'ominozzo2' parallel-ssh -h /home/salvo/Scrivania/PH-CMD/cdaCloud/cuhosts -l bufu -AIi "sudo -S /usr/share/elasticsearch/bin/plugin -r river-runriver" < /home/salvo/Scrivania/PH-CMD/cdaCloud/pass

echo -e '\nINSTALL NEW PLUGIN\n' #set VERSION!!!!!!!!!!!!!!!!!!!
sshpass -p 'ominozzo2' parallel-ssh -h /home/salvo/Scrivania/PH-CMD/cdaCloud/cuhosts -l bufu -AIi "sudo -S /usr/share/elasticsearch/bin/plugin -url file:/home/bufu/river-runriver-1.3.3-plugin.zip -i river-runriver" < /home/salvo/Scrivania/PH-CMD/cdaCloud/pass


echo -e '\nRESTART ES\n'
sshpass -p 'ominozzo2' parallel-ssh -h /home/salvo/Scrivania/PH-CMD/cdaCloud/cuhosts -l bufu -AIi "sudo -S service elasticsearch restart" < /home/salvo/Scrivania/PH-CMD/cdaCloud/pass

echo -e '\n\n'
