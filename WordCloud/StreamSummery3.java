package org.wso2.siddhi.extension.TwitterSentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.json.JSONObject;
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
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import com.clearspring.analytics.stream.ITopK;
import com.clearspring.analytics.stream.ScoredItem;

public class StreamSummery3 extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StreamSummery3.class);
    private int PassToOut = 50;
    private int MaxLength = 500;
    VariableExpressionExecutor VaribleExecutorText;
    CustomConcurrentStreamSummary3 tpK;
    CustomConcurrentStreamSummary3 tpK2;
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    @Override
    public void start() {
        // TODO Auto-generated method stub
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {

                tpK = new CustomConcurrentStreamSummary3<String>(MaxLength);
                logger.info("---------Win1 Starttttttttttttttt1111111111111111 in cloud---------------");
                if (tpK2 != null) {
                    ArrayList<String> newList;
                    List stuff = (List) tpK2.peekWithScores((int) tpK2.size());
                    String Processed = "";
                    for (int i = 0; i < (int) tpK2.size(); i++) {
                        JSONObject jsonObj = new JSONObject(stuff.get(i));
                        String Val = jsonObj.getString("item");
                        int count = jsonObj.getInt("count");
                        tpK.offer(Val, count);
                    }

                }
            }
        }, 0, 16, HOURS);
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                logger.info("---------Win2 Starttttttttttttttt1111111111111111 in cloud---------------");
                tpK2 = new CustomConcurrentStreamSummary3<String>(MaxLength);
            }
        }, 8, 16, HOURS);
    }

    @Override
    public void stop() {
        logger.info("---------Schedular Shut down---------------");
        scheduler.shutdownNow();
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
        // TODO Auto-generated method stub
        // long lStartTime = System.currentTimeMillis();
        String rawString;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        // boolean firstEle=false;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            rawString = (String) VaribleExecutorText.execute(streamEvent);
            String[] arr = rawString.split(" ");

            for (int i = 0; i < arr.length; i++) {
                if (tpK != null) {
                    if (arr[i] != " " && arr[i] != null) {
                        tpK.offer(arr[i].trim().toLowerCase());
                    }
                }
                if (tpK2 != null) {
                    if (arr[i] != " " && arr[i] != null) {
                        tpK2.offer(arr[i].trim().toLowerCase());

                    }
                }
            }

            int peek = 0;
            peek = (int) ((tpK.size() < PassToOut) ? tpK.size() : PassToOut);
            List stuff = (List) tpK.peekWithScores(peek);
            String Processed = "";
            for (int i = 0; i < peek; i++) {
                Processed = Processed.concat(stuff.get(i) + ";");
            }
            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
            clonedEvent = setAttributeText(clonedEvent, Processed);
            returnEventChunk.add(clonedEvent);

        }
        // long lEndTime = System.currentTimeMillis();
        // long difference = lEndTime - lStartTime;
        // System.out.println("ITopK3: " + difference);
        nextProcessor.process(returnEventChunk);
    }

    private StreamEvent setAttributeText(StreamEvent event, String val) {
        switch (VaribleExecutorText.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, VaribleExecutorText.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, VaribleExecutorText.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, VaribleExecutorText.getPosition()[3]);
            break;
        }
        return event;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 3)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            PassToOut = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            MaxLength = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
            VaribleExecutorText = (VariableExpressionExecutor) attributeExpressionExecutors[2];
        } else {
            throw new UnsupportedOperationException("The first parameter should be Tweet Text");
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        return attributeList;
    }
}

class CustomConcurrentStreamSummary3<T> implements ITopK<T> {
    private final int capacity;
    private final ConcurrentHashMap<T, ScoredItem<T>> itemMap;
    private final AtomicReference<ScoredItem<T>> minVal;
    private final AtomicLong size;
    private final AtomicBoolean reachCapacity;

    public CustomConcurrentStreamSummary3(final int capacity) {
        this.capacity = capacity;
        this.minVal = new AtomicReference<ScoredItem<T>>();
        this.size = new AtomicLong(0);
        this.itemMap = new ConcurrentHashMap<T, ScoredItem<T>>(capacity);
        this.reachCapacity = new AtomicBoolean(false);
    }

    @Override
    public boolean offer(final T element) {
        return offer(element, 1);
    }

    @Override
    public boolean offer(final T element, final int incrementCount) {
        long val = incrementCount;
        ScoredItem<T> value = new ScoredItem<T>(element, incrementCount);
        ScoredItem<T> oldVal = itemMap.putIfAbsent(element, value);
        if (oldVal != null) {
            val = oldVal.addAndGetCount(incrementCount);
        } else if (reachCapacity.get() || size.incrementAndGet() > capacity) {
            reachCapacity.set(true);

            ScoredItem<T> oldMinVal = minVal.getAndSet(value);
            itemMap.remove(oldMinVal.getItem());

            while (oldMinVal.isNewItem()) {
                // Wait for the oldMinVal so its error and value are completely up to date.
                // no thread.sleep here due to the overhead of calling it - the waiting time will be microseconds.
            }
            long count = oldMinVal.getCount();

            value.addAndGetCount(count);
            value.setError(count);
        }
        value.setNewItem(false);
        minVal.set(getMinValue());

        return val != incrementCount;
    }

    private ScoredItem<T> getMinValue() {
        ScoredItem<T> minVal = null;
        for (ScoredItem<T> entry : itemMap.values()) {
            if (minVal == null || (!entry.isNewItem() && entry.getCount() < minVal.getCount())) {
                minVal = entry;
            }
        }
        return minVal;
    }

    public long size() {
        return size.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ScoredItem entry : itemMap.values()) {
            sb.append("(" + entry.getCount() + ": " + entry.getItem() + ", e: " + entry.getError() + "),");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<T> peek(final int k) {
        List<T> toReturn = new ArrayList<T>(k);
        List<ScoredItem<T>> values = peekWithScores(k);
        for (ScoredItem<T> value : values) {
            toReturn.add(value.getItem());
        }
        return toReturn;
    }

    public List<ScoredItem<T>> peekWithScores(final int k) {
        List<ScoredItem<T>> values = new ArrayList<ScoredItem<T>>();
        for (Map.Entry<T, ScoredItem<T>> entry : itemMap.entrySet()) {
            ScoredItem<T> value = entry.getValue();
            values.add(new ScoredItem<T>(value.getItem(), value.getCount(), value.getError()));
        }
        Collections.sort(values);
        values = values.size() > k ? values.subList(0, k) : values;
        return values;
    }

}
