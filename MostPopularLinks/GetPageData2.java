package org.wso2.siddhi.extension.CustomExeFun;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class GetPageData2 extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GetPageData2.class);
    private VariableExpressionExecutor URLPg;
    ComplexEventChunk<StreamEvent> returnEventChunk;
    int corePoolSize = 8;
    int maxPoolSize = 300;
    long keepAliveTime = 20000;
    int jobQueueSize = 10000;
    BlockingQueue<Runnable> bounded;
    ExecutorService threadExecutor;

    @Override
    public void start() {
        // TODO Auto-generated method stub
        bounded = new LinkedBlockingQueue<Runnable>(jobQueueSize);
        threadExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                bounded);

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
        threadExecutor.shutdown();
        bounded.clear();
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
        returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;

        long lStartTime = System.currentTimeMillis();
        // TODO Auto-generated method stub
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            threadExecutor.submit(new jsoupConnection(streamEvent, complexEventPopulater));

        }
        sendEventChunk();

    }

    public synchronized void addEventChunk(StreamEvent event) {
        returnEventChunk.add(event);
    }

    public synchronized void sendEventChunk() {
        if (returnEventChunk.hasNext()) {
            nextProcessor.process(returnEventChunk);
            returnEventChunk = new ComplexEventChunk<StreamEvent>();
        }
    }

    private String getLink(Document doc, StringBuilder sb) {
        String text = null;
        Elements metaOgTitle = doc.select("meta[property=og:title]");
        if (metaOgTitle != null) {
            text = metaOgTitle.attr("content");
        } else {
            text = doc.title();
        }
        if (text == "") {
            text = doc.title();
        }
        if (text != null) {
            sb.append(text);
        }
        return sb.toString();
    }

    /*
     * private String getImage(Document doc, Connection con, StringBuilder sb, String urlStr) {
     * String image = "images/logo.png";
     * 
     * if (!urlStr.contains("www.youtube.com")) {
     * 
     * if (!doc.baseUri().contains("www.youtube.com")) {
     * // System.out.println(doc.baseUri());
     * String text = null;
     * Elements media = doc.select("[src]");
     * int i=0;
     * for (Element src : media) {
     * if (src.tagName().equals("img")) {
     * String temp = src.attr("abs:src");
     * if (temp.contains(".jpg") || temp.contains(".png") || temp.contains(".jpeg")
     * || temp.contains(".gif")) {
     * //image = temp;
     * // System.out.println(image);
     * if (temp.contains("pbs.twimg.com/media/") || temp.contains("web-master")) {
     * image = temp;
     * break;
     * }
     * 
     * if (urlStr.contains("www.breitbart.com")) {
     * image = temp;
     * break;
     * }
     * if (urlStr.contains("www.thegatewaypundit.com")) {
     * if (temp.contains("16004-presscdn-0-50.pagely.netdna-cdn.com")) {
     * image = temp;
     * break;
     * }
     * }
     * if (urlStr.contains("progressivemind.ucoz.com")) {
     * if (temp.contains("i.imgur.com")) {
     * image = temp;
     * break;
     * }
     * }
     * if (urlStr.contains("www.bbc.com") ||doc.baseUri().contains("www.bbc.com")) {
     * 
     * if(temp.contains("ichef-1")){
     * image = temp;
     * break;
     * }
     * }
     * if(i==media.size()/2){
     * image = temp;
     * }
     * i++;
     * }
     * }
     * }
     * } else {
     * String[] arr = urlStr.split("/");
     * image = "http://img.youtube.com/vi/" + arr[arr.length - 1] + "/default.jpg";
     * }
     * } else {
     * String[] arr = urlStr.split("=");
     * image = "http://img.youtube.com/vi/" + arr[arr.length - 1] + "/default.jpg";
     * }
     * 
     * return image;
     * }
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors.length != 1) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to GetPost() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            URLPg = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new UnsupportedOperationException("The first parameter should be an keyword String");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Title", Attribute.Type.STRING));
        attributeList.add(new Attribute("ImgUrl", Attribute.Type.STRING));;
        return attributeList;
    }

    class jsoupConnection implements Runnable {
        String urlStr;
        StreamEvent event;
        ComplexEventPopulater complexEventPopulater;

        public jsoupConnection(StreamEvent event, ComplexEventPopulater complexEventPopulater) {
            this.urlStr = (String) URLPg.execute(event);
            this.event = event;
            this.complexEventPopulater = complexEventPopulater;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            StringBuilder sb = new StringBuilder();
            Connection con = Jsoup
                    .connect(urlStr)
                    .userAgent(
                            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                    .ignoreContentType(true).ignoreHttpErrors(true).timeout(10000);
            Document doc;
            try {
                doc = con.get();

                String Title = getLink(doc, sb);
                // String Image =getImage(doc, con, sb, urlStr);//"images/logo.png";//getImage(doc, con, sb, urlStr);
                String Image = "images/logo.png";
                complexEventPopulater.populateComplexEvent(event, new Object[] { Title, Image });
                addEventChunk(event);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                logger.error("Error Extracting Link Data ", e.getMessage());
            }

        }

    }

}
