package org.neo4j.server.plugin.facebook;

import com.restfb.DefaultJsonMapper;
import com.restfb.DefaultWebRequestor;
import org.junit.*;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.types.User;

import java.io.IOException;
import java.net.HttpURLConnection;

public class FacebookPluginTest {
    private static final String MY_ACCESS_TOKEN = "MYTOKEN";

    @Test
    public void getUserTest() throws Exception{

        FacebookClient facebookClient = new DefaultFacebookClient(MY_ACCESS_TOKEN,

                // A one-off DefaultWebRequestor for testing that returns a hardcoded JSON
                // object instead of hitting the Facebook API endpoint URL

                new DefaultWebRequestor() {
                    @Override
                    public Response executeGet(String url) throws IOException {
                        return new Response(HttpURLConnection.HTTP_OK,
                                "{'id':'123456','name':'Test Person','gender':'male','username':'tester'}");
                    }
                }, new DefaultJsonMapper());

        User user = facebookClient.fetchObject("me", User.class, com.restfb.Parameter.with("fields", "id, name, gender, username"));
        assert "123456".equals(user.getId());
        assert "Test Person".equals(user.getName());
        assert "male".equals(user.getGender());
        assert "tester".equals(user.getUsername());

    }


}
