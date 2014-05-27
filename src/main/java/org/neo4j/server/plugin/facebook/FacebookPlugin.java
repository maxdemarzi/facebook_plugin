package org.neo4j.server.plugin.facebook;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.*;
import com.restfb.Connection;
import com.restfb.exception.FacebookException;
import com.restfb.json.JsonObject;
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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

@Description( "An extension to the Neo4j Server for retrieving Facebook Data" )
public class FacebookPlugin extends ServerPlugin {
    private final static Logger LOGGER = Logger.getLogger( FacebookPlugin.class.getName() );
    private static final int NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors();
    ListeningExecutorService service = MoreExecutors.listeningDecorator( Executors.newFixedThreadPool( NUMBER_OF_THREADS ) );

    private final PathFinder<Path> SHORTEST_PATH_LEVEL_ONE = GraphAlgoFactory
            .shortestPath( PathExpanders.forType( FacebookRelationshipTypes.FRIENDS ), 1 );

    private Node getOrCreateNode( GraphDatabaseService graphDb, String id, String token ) {
        Node userNode;
        try ( Transaction tx = graphDb.beginTx() ) {
            userNode = IteratorUtil.singleOrNull( graphDb.findNodesByLabelAndProperty( FacebookLabels.User, FacebookProperties.ID, id ) );
            if ( userNode == null ) {
                JsonObject user = getFacebookUser( id, token );

                userNode = graphDb.createNode();
                userNode.setProperty( FacebookProperties.ID, user.getString( "uid" ) );
                userNode.setProperty( FacebookProperties.FULL_NAME, Optional.fromNullable(user.getString( "name" ) ).or( "" ));
                userNode.setProperty( FacebookProperties.GENDER, Optional.fromNullable( user.getString( "sex" ) ).or( "" ) );
                userNode.setProperty( FacebookProperties.PIC_BIG, Optional.fromNullable( user.getString( "pic_big" ) ).or( "" ) );
                userNode.setProperty( FacebookProperties.PIC_SMALL, Optional.fromNullable( user.getString( "pic_small" ) ).or( "" ) );
                userNode.addLabel( FacebookLabels.User );
                tx.success();
            }
        }
        return userNode;
    }

    private static JsonObject getFacebookUser( String id, String token ) {
        FacebookClient facebookClient = new DefaultFacebookClient( token );
        String query = String.format( "SELECT uid, name, sex, pic_big, pic_small FROM user WHERE uid = %s", id );
        return facebookClient.executeFqlQuery( query, JsonObject.class ).get( 0 );
    }

    @Name( "install" )
    @Description( "Create Indexes and Constraints required for Plugin" )
    @PluginTarget( GraphDatabaseService.class )
    public String install( @Source GraphDatabaseService graphDb ) {

        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.constraintFor( FacebookLabels.User )
                    .assertPropertyIsUnique( "id" )
                    .create();
            schema.indexFor( FacebookLabels.User )
                    .on( FacebookProperties.FULL_NAME )
                    .create();
            schema.constraintFor( FacebookLabels.Place )
                    .assertPropertyIsUnique( "id" )
                    .create();
            schema.indexFor( FacebookLabels.Place )
                    .on( FacebookProperties.NAME )
                    .create();
            schema.constraintFor( FacebookLabels.Thing )
                    .assertPropertyIsUnique( "id" )
                    .create();
            schema.indexFor( FacebookLabels.Thing )
                    .on( FacebookProperties.NAME )
                    .create();
            schema.constraintFor( FacebookLabels.Group )
                    .assertPropertyIsUnique( "id" )
                    .create();
            schema.indexFor( FacebookLabels.Group )
                    .on( FacebookProperties.NAME )
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

    @Name( "import_user" )
    @Description( "Import Facebook User" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation importUser( final @Source GraphDatabaseService graphDb,
                                   @Description( "The Facebook User Id to Retrieve." )
                                   @Parameter( name = "id" ) String id,
                                   @Description( "The Facebook Access Token." )
                                   @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        try {
            Node userNode = getOrCreateNode(graphDb, id, token);
            representation = new NodeRepresentation( userNode );
        }  catch ( Exception e ) {
            representation = new ExceptionRepresentation( e );
        }
        return representation;
    }

    @Name( "import_user_async" )
    @Description( "Import Facebook User Asynchronously" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation importUserAsync( final @Source GraphDatabaseService graphDb,
                                      @Description( "The Facebook User Id to Retrieve." )
                                      @Parameter( name = "id" ) final String id,
                                      @Description( "The Facebook Access Token." )
                                      @Parameter( name = "token", optional = true ) final String token ) throws Exception
    {
        service.submit( new Callable<Node>() {
            public Node call() {
                return getOrCreateNode( graphDb, id, token );
            }
        });

        return ValueRepresentation.string( "Import User for " + id + " with token " + token + " received." );
    }

    @Name( "import_friends" )
    @Description( "Import Facebook Friends" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation importFriends( @Source GraphDatabaseService graphDb,
                                                              @Description( "The Facebook User Id to Retrieve." )
                                                              @Parameter( name = "id" ) String id,
                                                              @Description( "The Facebook Access Token." )
                                                              @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        ArrayList<Representation> friends = new ArrayList();
        Node userNode = getOrCreateNode( graphDb, id, token );

        try ( Transaction tx = graphDb.beginTx() ) {
            FacebookClient facebookClient = new DefaultFacebookClient( token );
            Connection<User> myFriends = facebookClient.fetchConnection( "me/friends", User.class);

                for ( List<User> myFriendsConnectionPage : myFriends ) {
                    for ( User friend : myFriendsConnectionPage ) {
                        Node friendNode = getOrCreateNode( graphDb, friend.getId(), token );
                        if ( SHORTEST_PATH_LEVEL_ONE.findSinglePath( friendNode, userNode ) == null ) {
                            userNode.createRelationshipTo( friendNode, FacebookRelationshipTypes.FRIENDS );
                        }
                        friends.add( new NodeRepresentation( friendNode ) );
                    }
                }

            tx.success();
            representation = new ListRepresentation( RepresentationType.NODE, friends );
 
        }  catch ( Exception e ) {
            representation = new ExceptionRepresentation( e );
        }
        return representation;
    }

    @Name( "import_friends_async" )
    @Description( "Import Facebook Friends Asynchronously" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation importFriendsAsync( final @Source GraphDatabaseService graphDb,
                                         @Description( "The Facebook User Id to Retrieve." )
                                         final @Parameter( name = "id" ) String id,
                                         @Description( "The Facebook Access Token." )
                                         final @Parameter( name = "token", optional = true ) String token )
    {
        service.submit( new Callable<ArrayList<Node>>() {
            public ArrayList<Node> call() {
                return getOrCreateFriends( graphDb, id, token );
            }
        });

        return ValueRepresentation.string( "Importing Friends for " + id + " with token " + token + " received." );
    }

    private ArrayList<Node> getOrCreateFriends(GraphDatabaseService graphDb, String id, String token) {
        ArrayList<Node> friends = new ArrayList();
        Node userNode = getOrCreateNode( graphDb, id, token );

        try ( Transaction tx = graphDb.beginTx() ) {
            FacebookClient facebookClient = new DefaultFacebookClient( token );
            Connection<User> myFriends = facebookClient.fetchConnection( "me/friends", User.class);

            for ( List<User> myFriendsConnectionPage : myFriends ) {
                for ( User friend : myFriendsConnectionPage ) {
                    Node friendNode = getOrCreateNode( graphDb, friend.getId(), token );
                    if ( SHORTEST_PATH_LEVEL_ONE.findSinglePath( friendNode, userNode ) == null ) {
                        userNode.createRelationshipTo( friendNode, FacebookRelationshipTypes.FRIENDS );
                    }
                    friends.add( friendNode );
                }
            }

            tx.success();

        }  catch (Exception e) {
            LOGGER.log( Level.SEVERE, e.toString(), e );
        }
        return friends;
    }


    @Name( "import_mutual_friends" )
    @Description( "Import Mutual Friends of Facebook Friends" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation getMutual( @Source GraphDatabaseService graphDb,
                                   @Description( "The Facebook User Id to Retrieve." )
                                   @Parameter( name = "id" ) String id,
                                   @Description( "The Facebook Access Token." )
                                   @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;

        try ( Transaction tx = graphDb.beginTx() ) {
            Node userNode = getOrCreateNode( graphDb, id, token );

            for ( Relationship friendRel : userNode.getRelationships( FacebookRelationshipTypes.FRIENDS ) ){
                Node friendNode = friendRel.getOtherNode( userNode );
                String friendId = (String)friendNode.getProperty( FacebookProperties.ID);

                try {
                    FacebookClient facebookClient = new DefaultFacebookClient( token );
                    Connection<User> myFriends = facebookClient.fetchConnection( "me/mutualfriends/" + friendId, User.class );

                    for ( List<User> myFriendsConnectionPage : myFriends ) {
                        for ( User friend : myFriendsConnectionPage ) {
                            Node fofNode = getOrCreateNode( graphDb, friend.getId(), token );
                            if (SHORTEST_PATH_LEVEL_ONE.findSinglePath( fofNode, friendNode ) == null ) {
                                friendNode.createRelationshipTo( fofNode, FacebookRelationshipTypes.FRIENDS );
                            }
                        }
                    }
                } catch ( FacebookException e) {
                    LOGGER.log( Level.SEVERE, e.toString(), e );
                }

            }
            tx.success();
            representation = ValueRepresentation.string( "Imported Mutual Friends" );

        }  catch ( Exception e ) {
            representation = new ExceptionRepresentation( e );
        }
        return representation;
    }

}