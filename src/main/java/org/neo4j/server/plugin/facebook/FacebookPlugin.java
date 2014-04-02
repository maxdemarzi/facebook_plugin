package org.neo4j.server.plugin.facebook;

import com.google.common.base.Optional;
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
import java.util.concurrent.TimeUnit;

@Description( "An extension to the Neo4j Server for getting all nodes or relationships" )
public class FacebookPlugin extends ServerPlugin {

    private static final String ID = "id";
    private static final String FULL_NAME = "name";
    private static final String USER_NAME = "username";
    private static final String GENDER = "gender";
    private static final String BIRTHDAY = "birthday";
    private static final String RELATIONSHIP_STATUS = "relationship_status";
    private static final String PIC_BIG = "pic_big";
    private static final String PIC_SMALL = "pic_small";

    private final PathFinder<Path> SHORTEST_PATH_LEVEL_ONE = GraphAlgoFactory
            .shortestPath( PathExpanders.forType( FacebookRelationshipTypes.FRIENDS ), 1 );


    private Node getOrCreateNode( GraphDatabaseService graphDb, String id, String token ) {
        Node userNode = IteratorUtil.singleOrNull( graphDb.findNodesByLabelAndProperty( FacebookLabels.User, ID, id ) );
        if ( userNode == null ) {
            FacebookClient facebookClient = new DefaultFacebookClient( token );
            User user = facebookClient.fetchObject( id, User.class, com.restfb.Parameter.with( "fields", "id, name, gender, username" ) );
            userNode = graphDb.createNode();
            userNode.setProperty( ID, user.getId() );
            userNode.setProperty( FULL_NAME, Optional.fromNullable( user.getName() ).or( "" ) );
            userNode.setProperty( GENDER, Optional.fromNullable( user.getGender() ).or("") );
            userNode.setProperty( USER_NAME, Optional.fromNullable( user.getUsername() ).or("") );
            userNode.setProperty( BIRTHDAY, Optional.fromNullable( user.getBirthday() ).or( "" ) );
            userNode.addLabel( FacebookLabels.User );
        }
        return userNode;
    }

    private Node getOrCreateNodeFQL( GraphDatabaseService graphDb, String id, String token ) {
        Node userNode = IteratorUtil.singleOrNull( graphDb.findNodesByLabelAndProperty( FacebookLabels.User, ID, id ) );
        if ( userNode == null ) {
            FacebookClient facebookClient = new DefaultFacebookClient( token );
            String query = String.format("SELECT uid, name, sex, birthday_date, relationship_status, pic_big, pic_small FROM user WHERE uid = %s", id);
            JsonObject user = facebookClient.executeFqlQuery(query, JsonObject.class).get(0);

            userNode = graphDb.createNode();
            userNode.setProperty( ID, user.getString( "uid" ) );
            userNode.setProperty( FULL_NAME, Optional.fromNullable(user.getString( "name" )).or(""));
            userNode.setProperty( GENDER, Optional.fromNullable( user.getString( "sex" ) ).or("") );
            userNode.setProperty( BIRTHDAY, Optional.fromNullable( user.getString( "birthday_date") ).or("") );
            userNode.setProperty( RELATIONSHIP_STATUS, Optional.fromNullable( user.getString( "relationship_status" ) ).or("") );
            userNode.setProperty( PIC_BIG, Optional.fromNullable( user.getString( "pic_big" ) ).or("") );
            userNode.setProperty( PIC_SMALL, Optional.fromNullable( user.getString( "pic_small" ) ).or("") );
            userNode.addLabel( FacebookLabels.User );
        }
        return userNode;
    }
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

    @Name( "import_user" )
    @Description( "Import Facebook User" )
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

    @Name( "import_user_by_fql" )
    @Description( "Import Facebook User" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation getUserFQL( @Source GraphDatabaseService graphDb,
                                   @Description( "The Facebook User Id to Retrieve." )
                                   @Parameter( name = "id" ) String id,
                                   @Description( "The Facebook Access Token." )
                                   @Parameter( name = "token", optional = true ) String token )
    {
        Representation representation;
        try ( Transaction tx = graphDb.beginTx() ) {
            Node userNode = getOrCreateNodeFQL(graphDb, id, token);
            tx.success();
            representation = new NodeRepresentation( userNode );

        }  catch (Exception e) {
            representation = new ExceptionRepresentation(e);
        }
        return representation;
    }

    @Name( "import_friends" )
    @Description( "Import Facebook Friends" )
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
                Node friendNode = friendRel.getOtherNode(userNode);
                String friendId = (String)friendNode.getProperty(ID);

                try {
                    FacebookClient facebookClient = new DefaultFacebookClient( token );
                    Connection<User> myFriends = facebookClient.fetchConnection("me/mutualfriends/" + friendId, User.class);

                    for ( List<User> myFriendsConnectionPage : myFriends ) {
                        for ( User friend : myFriendsConnectionPage ) {
                            Node fofNode = getOrCreateNode( graphDb, friend.getId(), token );
                            if (SHORTEST_PATH_LEVEL_ONE.findSinglePath( fofNode, friendNode ) == null ) {
                                friendNode.createRelationshipTo( fofNode, FacebookRelationshipTypes.FRIENDS );
                            }
                        }
                    }
                } catch (FacebookException e) {
                    System.out.println(e);
                }

            }
            tx.success();
            representation = ValueRepresentation.string("Imported Mutual Friends");

        }  catch (Exception e) {
            representation = new ExceptionRepresentation(e);
        }
        return representation;
    }

}