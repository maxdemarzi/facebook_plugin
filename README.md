Neo4j Facebook Plugin
---------------------

A Neo4j Server plugin to make it easier to work with the Facebook Graph.


Instructions


    mvn install
    unzip target/facebook_plugin-1.0-SNAPSHOT-server-plugin.zip -d neo4j/plugins/
    neo4j/bin/neo4j restart
    curl -X POST http://localhost:7474/db/data/ext/FacebookPlugin/graphdb/install

Import data using the Graph API:

    curl -X POST http://localhost:7474/db/data/ext/FacebookPlugin/graphdb/import_user -H "Content-Type: application/json"   -d '{"id":"your fb id",
           "token": "your fb token"}'
    curl -X POST http://localhost:7474/db/data/ext/FacebookPlugin/graphdb/import_friends -H "Content-Type: application/json"   -d '{"id":"your fb id",
           "token": "your fb token"}'
    curl -X POST http://localhost:7474/db/data/ext/FacebookPlugin/graphdb/import_mutual_friends -H "Content-Type: application/json"   -d '{"id":"your fb id",
           "token": "your fb token"}'

Import data using FQL:

    curl -X POST http://localhost:7474/db/data/ext/FacebookPlugin/graphdb/import_user_by_fql -H "Content-Type: application/json"   -d '{"id":"your fb id",
           "token": "your fb token"}'


