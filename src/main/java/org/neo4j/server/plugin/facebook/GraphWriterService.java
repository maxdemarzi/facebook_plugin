package org.neo4j.server.plugin.facebook;


import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.restfb.json.JsonObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

class GraphWriterService extends AbstractScheduledService {
    private Logger logger = Logger.getLogger("org.neo4j.server.plugin.facebook");
    public GraphDatabaseService graphDb;
    public LinkedBlockingQueue<HashMap> toWrite = new LinkedBlockingQueue<>();

    protected void runOneIteration() throws Exception {
        logger.info("Inside runOneIteration");
        Collection<HashMap> writes = new ArrayList<>();
        toWrite.drainTo(writes);
        logger.info("Drained writes");
        for( HashMap write : writes){
            logger.info("Inside Writes");
            switch ((GraphWriterServiceAction) write.get("action")) {
                case CREATE_NODE:
                    logger.info("Inside Create Node");
                    try ( Transaction tx = graphDb.beginTx() ) {
                        JsonObject user = (JsonObject)write.get("user");
                        Node userNode = graphDb.createNode();
                        userNode.setProperty( FacebookProperties.ID, user.getString( "uid" ) );
                        userNode.setProperty( FacebookProperties.FULL_NAME, Optional.fromNullable(user.getString("name")).or(""));
                        userNode.setProperty( FacebookProperties.GENDER, Optional.fromNullable( user.getString( "sex" ) ).or("") );
                        userNode.setProperty( FacebookProperties.BIRTHDAY, Optional.fromNullable( user.getString( "birthday_date") ).or("") );
                        userNode.setProperty( FacebookProperties.RELATIONSHIP_STATUS, Optional.fromNullable( user.getString( "relationship_status" ) ).or("") );
                        userNode.setProperty( FacebookProperties.PIC_BIG, Optional.fromNullable( user.getString( "pic_big" ) ).or("") );
                        userNode.setProperty( FacebookProperties.PIC_SMALL, Optional.fromNullable( user.getString( "pic_small" ) ).or("") );
                        userNode.addLabel( FacebookLabels.User );
                        tx.success();
                        logger.info("TX Succeeded ");
                    }
            }
        }

    }

    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
    }
}