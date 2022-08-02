# Power shell
echo "Starting docker container (and building image, if necessary)..."
docker run `
    --name pskg `
    -p 7474:7474 -p 7687:7687 -p 7473:7473 `
    -d `
    -v $HOME/neo4j/data:/data `
    -v $HOME/neo4j/logs:/logs `
    -v $HOME/neo4j/import:/var/lib/neo4j/import `
    -v $HOME/neo4j/plugins:/plugins `
    --env NEO4J_AUTH=neo4j/pskg `
    --env NEO4J_dbms_connector_https_advertised__address="localhost:7473" `
    --env NEO4J_dbms_connector_http_advertised__address="localhost:7474" `
    --env NEO4J_dbms_connector_bolt_advertised__address="localhost:7687" `
    --env NEO4J_dbms_memory_heap_max__size="16G" `
    --env NEO4J_dbms_memory_heap_initial__size="12G" `
    --env NEO4J_dbms_memory_pagecache_size="4G" `
    --env NEO4J_dbms_transaction_concurrent_maximum=0 `
    --env NEO4J_ACCEPT_LICENSE_AGREEMENT="yes" `
    --env NEO4J_apoc_export_file_enabled=true `
    --env NEO4J_apoc_import_file_enabled=true `
    --env NEO4J_apoc_import_file_use__neo4j__config=true `
    --env NEO4JLABS_PLUGINS=[`"apoc`"] `
    --env NEO4J_dbms_security_procedures_unrestricted=apoc.`\`* `
   neo4j: # Here enter your 4.4.4-enterprise configuration
    