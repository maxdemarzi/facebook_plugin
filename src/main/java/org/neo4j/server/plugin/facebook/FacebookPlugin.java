package org.neo4j.server.plugin.facebook;

import com.google.common.base.Optional;
import com.restfb.Connection;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.plugins.*;
import org.neo4j.server.rest.repr.*;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.types.User;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Description( "An extension to the Neo4j Server for getting all nodes or relationships" )
public class FacebookPlugin extends ServerPlugin {

    private static final String ID = "id";
    private static final String FULL_NAME = "name";
    private static final String USER_NAME = "username";
    private static final String GENDER = "gender";

    private final PathFinder<Path> SHORTEST_PATH_LEVEL_ONE = GraphAlgoFactory
            .shortestPath( PathExpanders.forType( FacebookRelationshipTypes.FRIENDS ), 1 );

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
    public Representation getUser( @Source GraphDatabaseService graphDb,
                         @Description( "The Facebook User Id to Retrieve." )
                         @Parameter( name = "id" ) String id,
                         @Description( "The Facebook Access Token." )
                         @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        try ( Transaction tx = graphDb.beginTx() ) {
            Node userNode = getOrCreateNode( graphDb, id, token );
            tx.success();
            representation = new NodeRepresentation( userNode );

        }  catch (Exception e) {
            representation = new ExceptionRepresentation(e);
        }
        return representation;
    }

    private Node getOrCreateNode( GraphDatabaseService graphDb, String id, String token ) {
        Node userNode = IteratorUtil.singleOrNull( graphDb.findNodesByLabelAndProperty( FacebookLabels.User, ID, id ) );
        if ( userNode == null ) {
            FacebookClient facebookClient = new DefaultFacebookClient( token );
            User user = facebookClient.fetchObject( id, User.class, com.restfb.Parameter.with( "fields", "id, name, gender, username" ) );
            userNode = graphDb.createNode();
            userNode.setProperty( ID, user.getId() );
            userNode.setProperty( FULL_NAME, Optional.fromNullable( user.getName() ).or( "" ) );
            userNode.setProperty( GENDER, Optional.fromNullable( user.getGender() ).or( "" ) );
            userNode.setProperty( USER_NAME, Optional.fromNullable( user.getUsername() ).or( "" ) );
            userNode.addLabel( FacebookLabels.User );
        }
        return userNode;
    }

    @Name( "get_friends" )
    @Description( "Get Facebook Friends" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation getFriends( @Source GraphDatabaseService graphDb,
                                                              @Description( "The Facebook User Id to Retrieve." )
                                                              @Parameter( name = "id" ) String id,
                                                              @Description( "The Facebook Access Token." )
                                                              @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        ArrayList<Representation> friends = new ArrayList();

        try ( Transaction tx = graphDb.beginTx() ) {
            Node userNode = getOrCreateNode( graphDb, id, token );

            FacebookClient facebookClient = new DefaultFacebookClient( token );
            Connection<User> myFriends = facebookClient.fetchConnection("me/friends", User.class);

                for ( List<User> myFriendsConnectionPage : myFriends ) {
                    for ( User friend : myFriendsConnectionPage ) {
                        Node friendNode = getOrCreateNode( graphDb, friend.getId(), token );
                        if (SHORTEST_PATH_LEVEL_ONE.findSinglePath( friendNode, userNode ) == null ) {
                            userNode.createRelationshipTo( friendNode, FacebookRelationshipTypes.FRIENDS );
                        }
                        friends.add(new NodeRepresentation(friendNode));
                    }
                }

            tx.success();
            representation = new ListRepresentation(RepresentationType.NODE, friends );
 
        }  catch (Exception e) {
            representation = new ExceptionRepresentation(e);
        }
        return representation;
    }
}