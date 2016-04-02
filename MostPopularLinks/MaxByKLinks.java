package org.wso2.siddhi.extension.CustomWin;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorLong;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

/*
 * #Customwindow.maxByKLinks('K', "ln(R)-kt", "*",'Tweet text','rank')
 * Sample Query:
 * from inputStream#Customwindow.maxByK(200,5, "ln(R)-kt", "*",'Tweet text','rank')
 * select attribute1, attribute2
 * insert into outputStream;
 * */

public class MaxByKLinks extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MaxByKLinks.class);
    private static DivideExpressionExecutorDouble constantFunctionExecutor;
    private int PassToOut;
    private int Lengthtokeep;
    private ArrayList<StreamEvent> sortedWindow = new ArrayList<StreamEvent>();
    private EventComparator eventComparator;
    private VariableExpressionExecutor variableExpressionExecutor;
    private VariableExpressionExecutor variableExpressionCount;
    private VariableExpressionExecutor variableExpressionRank;
    private VariableExpressionExecutor variableExpressionRt;
    private VariableExpressionExecutor variableExpressionFt;
    private VariableExpressionExecutor variableExpressionOwner;

    private class EventComparator implements Comparator<StreamEvent> {
        @Override
        public int compare(StreamEvent e1, StreamEvent e2) {
            int comparisonResult;
            int[] variablePosition = ((VariableExpressionExecutor) variableExpressionRank).getPosition();
            Comparable comparableVariable1 = (Comparable) e1.getAttribute(variablePosition);
            Comparable comparableVariable2 = (Comparable) e2.getAttribute(variablePosition);
            comparisonResult = comparableVariable1.compareTo(comparableVariable2);
            if (comparisonResult != 0) {
                return (-1) * comparisonResult;
            } else
                return 0;
        }
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
        return new Object[] { sortedWindow };
    }

    @Override
    public void restoreState(Object[] state) {
        // TODO Auto-generated method stub
        sortedWindow = (ArrayList<StreamEvent>) state[0];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        // TODO Auto-generated method stub
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        Double r = -1.0;
        // boolean firstEle=false;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            for (int i = 0; i < sortedWindow.size(); i++) {
                try {
                    r = (Double) constantFunctionExecutor.execute(sortedWindow.get(i));
                } catch (ClassCastException e) {
                    e.getMessage();
                }
                setAttributeRank(sortedWindow.get(i), r);
            }
            try {
                r = (Double) constantFunctionExecutor.execute(streamEvent);

            } catch (ClassCastException e) {
                e.getMessage();
            }
            streamEvent = setAttributeRank(streamEvent, r);
            boolean duplicate = false;
            if ((sortedWindow.size() < Lengthtokeep)) {
                duplicate = IsDuplicate(streamEvent);
                if (duplicate == false) {
                    sortedWindow.add(streamEvent);
                    Collections.sort(sortedWindow, eventComparator);
                }
            } else if (((Double) variableExpressionRank.execute(sortedWindow.get(sortedWindow.size() - 1)) > (Double) variableExpressionRank
                    .execute(streamEvent)) || (variableExpressionExecutor.execute(streamEvent).equals("null"))) {
                continue;
            } else {
                duplicate = IsDuplicate(streamEvent);
                if (duplicate == false) {
                    sortedWindow.remove(sortedWindow.size() - 1);
                    sortedWindow.add(streamEvent);
                    Collections.sort(sortedWindow, eventComparator);
                }

                for (int j = 0; j < PassToOut; j++) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(sortedWindow.get(j));
                    complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { j + 1 });
                    returnEventChunk.add(clonedEvent);
                }
                nextProcessor.process(returnEventChunk);
            }
        }

    }

    private StreamEvent setAttributeRank(StreamEvent event, double val) {
        switch (variableExpressionRank.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionRank.getPosition()[3]);
            break;
        }
        return event;
    }

    private StreamEvent setAttributeRt(StreamEvent event, int val) {
        switch (variableExpressionRt.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionRt.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionRt.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionRt.getPosition()[3]);
            break;
        }
        return event;
    }

    private StreamEvent AddAttributeOwner(StreamEvent event, String val) {
        switch (variableExpressionOwner.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionOwner.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionOwner.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionOwner.getPosition()[3]);
            break;
        }
        return event;
    }

    private StreamEvent setAttributeFt(StreamEvent event, int val) {
        switch (variableExpressionFt.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionFt.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionFt.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionFt.getPosition()[3]);
            break;
        }
        return event;
    }
//=====Main Logic===============================================================================
    private boolean IsDuplicate(StreamEvent event) {
        boolean duplicate = false;
        for (int i = sortedWindow.size() - 1; i >= 0; i--) {
            if (variableExpressionExecutor.execute(sortedWindow.get(i)).equals(
                    variableExpressionExecutor.execute(event))
                    && ((String) variableExpressionOwner.execute(sortedWindow.get(i)))
                            .contains((String) variableExpressionOwner.execute(event))) {
                setAttributeRt(sortedWindow.get(i), 1 + (Integer) variableExpressionRt.execute(sortedWindow.get(i)));
                setAttributeRank(sortedWindow.get(i), (Double) constantFunctionExecutor.execute(sortedWindow.get(i)));
                Collections.sort(sortedWindow, eventComparator);
                duplicate = true;
            } else if (variableExpressionExecutor.execute(sortedWindow.get(i)).equals(
                    variableExpressionExecutor.execute(event))) {
                setAttributeRt(sortedWindow.get(i), (Integer) variableExpressionRt.execute(event)
                        + (Integer) variableExpressionRt.execute(sortedWindow.get(i)));
                setAttributeFt(sortedWindow.get(i), (Integer) variableExpressionRt.execute(event)
                        + (Integer) variableExpressionRt.execute(sortedWindow.get(i)));
                AddAttributeOwner(sortedWindow.get(i), (String) variableExpressionOwner.execute(sortedWindow.get(i))
                        + " , @" + (String) variableExpressionOwner.execute(event));
                setAttributeRank(sortedWindow.get(i), (Double) constantFunctionExecutor.execute(sortedWindow.get(i)));
                Collections.sort(sortedWindow, eventComparator);
                duplicate = true;
            }

        }
        return duplicate;
    }
//================================================================================================
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 9)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Lengthtokeep = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            PassToOut = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (!(attributeExpressionExecutors[2] instanceof DivideExpressionExecutorDouble)) {
            throw new UnsupportedOperationException(
                    "Required a Devide Function Executor, but found a other Function Type");
        } else {
            constantFunctionExecutor = (DivideExpressionExecutorDouble) attributeExpressionExecutors[2];
        }
        if (!(attributeExpressionExecutors[3] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a string parameter");
        } else {
            variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[3];
        }
        if (!(attributeExpressionExecutors[4] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionRank = (VariableExpressionExecutor) attributeExpressionExecutors[4];
        }
        if (!(attributeExpressionExecutors[5] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionCount = (VariableExpressionExecutor) attributeExpressionExecutors[5];
        }

        if (!(attributeExpressionExecutors[6] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionFt = (VariableExpressionExecutor) attributeExpressionExecutors[6];
        }
        if (!(attributeExpressionExecutors[7] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionRt = (VariableExpressionExecutor) attributeExpressionExecutors[7];
        }
        if (!(attributeExpressionExecutors[8] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionOwner = (VariableExpressionExecutor) attributeExpressionExecutors[8];
        }
        eventComparator = new EventComparator();
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Index", Attribute.Type.INT));
        return attributeList;

    }

}
