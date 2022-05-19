cd ../json
./drop.sh
sleep 5
./crtable.sh
sleep 20
cd ../rdf
./loader -g Movies -f ../data/movie-50.rdf > m50.log &

