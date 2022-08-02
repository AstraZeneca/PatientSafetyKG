#!/bin/bash 
echo "Extracting data files in $1 to container..."
cd $1
tar -cvf - * | docker cp - pskg:/var/lib/neo4j/import
cd -

echo "Copying load.cypher to container..."
docker cp neo4j/load_data/load.cypher pskg:/var/lib/neo4j/import

echo "done!"
