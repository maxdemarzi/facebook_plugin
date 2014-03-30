package org.neo4j.server.plugin.facebook;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.plugins.*;
import org.neo4j.server.rest.repr.*;

import com.restfb.DefaultFacebookClient;
import com.restfb.Facebook;
import com.restfb.FacebookClient;
import com.restfb.types.User;

import java.util.concurrent.TimeUnit;

@Description( "An extension to the Neo4j Server for getting all nodes or relationships" )
public class FacebookPlugin extends ServerPlugin {

    private static final String ID = "id";
    private static final String FULL_NAME = "name";
    private static final String USER_NAME = "username";
    private static final String GENDER = "gender";

    @Name( "install" )
    @Description( "Create Indexes and Constraints required for Plugin")
    @PluginTarget( GraphDatabaseService.class )
    public String install( @Source GraphDatabaseService graphDb) {

        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.constraintFor(FacebookLabels.User )
                    .assertPropertyIsUnique("id")
                    .create();
            schema.indexFor( FacebookLabels.User )
                    .on( FULL_NAME )
                    .create();
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.awaitIndexesOnline(10, TimeUnit.SECONDS);
            tx.success();
        }

        return "Facebook Plugin is installed!";
    }

    @Name( "get_user" )
    @Description( "Get Facebook User" )
    @PluginTarget( GraphDatabaseService.class )
    public org.neo4j.server.rest.repr.Representation getUser( @Source GraphDatabaseService graphDb,
                         @Description( "The Facebook User Id to Retrieve." )
                         @Parameter( name = "id" ) String id,
                         @Description( "The Facebook Access Token." )
                         @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        try (Transaction tx = graphDb.beginTx()) {
            Node userNode = IteratorUtil.singleOrNull(graphDb.findNodesByLabelAndProperty(FacebookLabels.User, ID, id));
            if ( userNode == null ) {

                FacebookClient facebookClient = new DefaultFacebookClient( token );
                User user = facebookClient.fetchObject(id, User.class, com.restfb.Parameter.with("fields", "id, name, gender, username"));
                userNode = graphDb.createNode();
                userNode.setProperty( ID, user.getId() );
                userNode.setProperty( FULL_NAME, user.getName() );
                userNode.setProperty( GENDER, user.getGender() );
                userNode.setProperty( USER_NAME, user.getUsername() );

                tx.success();
            }
            representation = new NodeRepresentation( userNode );

        }  catch (Exception e) {
            representation = new ExceptionRepresentation(e);
        }
        return representation;
    }

}