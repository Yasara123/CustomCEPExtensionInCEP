package org.wso2.siddhi.extension.CustomExeFun;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class FacebookCommentStream extends StreamProcessor {

    private String KeyWord;
    private int passToOut;
    private String accessTokenString;

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

    @Override
    public Object[] currentState() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent = streamEventChunk.getFirst();
        String fromUser;
        String postText = "";
        String CommentText;
        int likes;
        String CommentTime;
        String PostTime = "";
        // TODO Auto-generated method stub
        if (KeyWord != null) {

            try {
                URL url = new URL(
                        "https://graph.facebook.com/v2.5/"
                                + KeyWord
                                + "?fields=id%2Cname%2Cposts.limit(1)%7Bcomments.limit(200)%7Blike_count%2Ccreated_time%2Cfrom%2Cmessage%7D%2Cmessage%2Ccreated_time%7D&access_token="
                                + accessTokenString);
                InputStream is = url.openStream();
                JsonReader rdr = Json.createReader(is);
                JsonObject obj = (JsonObject) rdr.readObject();
                JsonArray results = (JsonArray) obj.getJsonObject("posts").get("data");
                int i = 0;
                if(results!=null){
                for (JsonObject result : results.getValuesAs(JsonObject.class)) {
                    if (result.containsKey("message")) {
                        postText = result.getString("message");
                    }
                    if (result.containsKey("created_time")) {
                        PostTime = result.getString("created_time");
                    }

                    if (result.containsKey("comments")) {
                        JsonArray comenteds = (JsonArray) result.getJsonObject("comments").get("data");
                        for (JsonObject comented : comenteds.getValuesAs(JsonObject.class)) {
                            fromUser = comented.getJsonObject("from").getString("name");
                            CommentText = comented.getString("message");
                            likes = comented.getInt("like_count");
                            CommentTime = comented.getString("created_time");
                            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                            complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] {fromUser,CommentText,postText,PostTime,CommentTime,likes,KeyWord});
                            returnEventChunk.add(clonedEvent);
                            nextProcessor.process(returnEventChunk);
                        }
                    }               
                }
                }
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } else {
            throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function cannot be null");
        }

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors.length != 3) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to GetPost() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            KeyWord = ((String) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an keyword String");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            passToOut = (Integer) (attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The 2nd parameter should be an integer pass to out ");
        }
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            accessTokenString = (String) (attributeExpressionExecutors[2].execute(null));
        } else {
            throw new UnsupportedOperationException("The 3rd parameter should be an acess token");
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("From_User", Attribute.Type.STRING));
        attributeList.add(new Attribute("Text", Attribute.Type.STRING));
        attributeList.add(new Attribute("ParentPostText", Attribute.Type.STRING));
        attributeList.add(new Attribute("ParentPostTime", Attribute.Type.STRING));
        attributeList.add(new Attribute("Created_Time", Attribute.Type.STRING));
        attributeList.add(new Attribute("LikesCount", Attribute.Type.INT));
        attributeList.add(new Attribute("Tag", Attribute.Type.STRING));
        return attributeList;
    }
}
