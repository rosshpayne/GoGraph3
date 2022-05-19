cd ../json
./drop.sh
sleep 5
./crtable.sh
sleep 20
cd ../rdf
./loader -g Relationship -c 1
cd ../attach
./attach -g Relationship 
cd ../dp

