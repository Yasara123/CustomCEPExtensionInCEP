package org.wso2.siddhi.extension.ClearSentiments;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.executor.math.Subtract.SubtractExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorDouble;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

/*
 * #Customwindow.maxByK('K', "ln(R)-kt", "*",'Tweet text','rank')
 * Sample Query:
 * from inputStream#Customwindow.maxByK(200,5, "ln(R)-kt", "*",'Tweet text','rank')
 * select attribute1, attribute2
 * insert into outputStream;
 * */

public class ClrSentiment extends FunctionExecutor{

    @Override
    public Type getReturnType() {
        // TODO Auto-generated method stub
        return Type.STRING;
    }

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
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors.length != 4) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to ClearSentiment:TrackWord() function, " +
                    "required 2, but found " + attributeExpressionExecutors.length);
        }
        Attribute.Type attributeType1 = attributeExpressionExecutors[0].getReturnType();
        if (!(attributeType1 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType2 = attributeExpressionExecutors[1].getReturnType();
        if (!(attributeType2 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType3 = attributeExpressionExecutors[2].getReturnType();
        if (!(attributeType3 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType4 = attributeExpressionExecutors[3].getReturnType();
        if (!(attributeType4 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
       
        
    }

    @Override
    protected Object execute(Object[] data) {
        String text="";      
        if (data != null) {
            String [] arr=((String) data[0]).split("\\.");
            for (String line : arr) {
                if((line.contains((String) data[1]))||(line.contains((String) data[2]))||(line.contains((String) data[3]))){
                    text=text.concat(line);
                }
            }
        }else {
            throw new ExecutionPlanRuntimeException("Input to the ClearSentiment:TrackWord() function cannot be null");
        }
        // TODO Auto-generated method stub
        return text;
    }

    @Override
    protected Object execute(Object data) {
       
        return null;
    }
  
}
