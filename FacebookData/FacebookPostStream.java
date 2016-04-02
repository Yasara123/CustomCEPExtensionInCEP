package org.wso2.siddhi.extension.CustomExeFun;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

public class FacebookPostStream extends StreamProcessor {
    private String KeyWord;
    private String accessTokenString;
    private int k=10;

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
                URL url = new URL("https://graph.facebook.com/v2.5/"+KeyWord+"/posts?limit="+k+"&access_token="+accessTokenString);
                InputStream is = url.openStream();
                JsonReader rdr = Json.createReader(is);
                JsonObject obj = (JsonObject) rdr.readObject();
                JsonArray results = (JsonArray) obj.get("data");
                if(results!=null){
                int i = 0;
                for (JsonObject result : results.getValuesAs(JsonObject.class)) {
                    if (result.containsKey("message")) {
                        postText = result.getString("message");
                        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] {postText});
                        returnEventChunk.add(clonedEvent);
                        nextProcessor.process(returnEventChunk);
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
            accessTokenString = (String) (attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The 3rd parameter should be an acess token");
        }
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            k = (Integer)(attributeExpressionExecutors[2].execute(null));
        } else {
            throw new UnsupportedOperationException("The 3rd parameter should be an acess token");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Text", Attribute.Type.STRING));
        return attributeList;
    }
}
 
