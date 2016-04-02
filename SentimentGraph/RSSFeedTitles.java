package org.wso2.siddhi.extension.RSSReader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
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

public class RSSFeedTitles extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RSSFeedTitles.class);
    private String URLSting;
    private URL rssURL;
    private int passToOut;
    String[] Titles;
    String[] PubDates;
    String[] Discriptions;
    String[] Links;

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
        // TODO Auto-generated method stub
        if (URLSting != null) {
            // type-conversion
            if (((String) URLSting).contains("https:"))
                try {
                    setURL(new URL((String) URLSting));
                    writeFeed();
                    int min = (passToOut > Titles.length) ? Titles.length : passToOut;
                    // System.out.print(min);
                    for (int i = 0; (i < min); i++) {
                        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { Titles[i],
                                Discriptions[i], PubDates[i], Links[i], (i + 1) });
                        returnEventChunk.add(clonedEvent);
                        nextProcessor.process(returnEventChunk);
                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                    logger.error("error in read title " + e.getMessage());
                }
            else {
                throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function should contain URL");
            }
        } else {
            throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function cannot be null");
        }

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to math:sin() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            URLSting = ((String) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an String");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            passToOut = (Integer) (attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The 2 parameter should be an integer");
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Title", Attribute.Type.STRING));
        attributeList.add(new Attribute("Dis", Attribute.Type.STRING));
        attributeList.add(new Attribute("Pub", Attribute.Type.STRING));
        attributeList.add(new Attribute("Link", Attribute.Type.STRING));
        attributeList.add(new Attribute("Count", Attribute.Type.INT));
        return attributeList;
    }

    public void setURL(URL url) {
        rssURL = url;
    }

    public void writeFeed() {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(rssURL.openStream());
            NodeList items = doc.getElementsByTagName("item");
            Titles = new String[items.getLength()];
            PubDates = new String[items.getLength()];
            Discriptions = new String[items.getLength()];
            Links = new String[items.getLength()];
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                Titles[i] = (String) getValue(item, "title");
                PubDates[i] = (String) getValue(item, "pubDate");
                Discriptions[i] = (String) getValue(item, "description");
                Links[i] = (String) getValue(item, "link");
            }

        } catch (Exception e) {
            // e.printStackTrace();
            logger.error("error in read title Connecting " + e.getMessage());
        }

    }

    public String getValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

}
