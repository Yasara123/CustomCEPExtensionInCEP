package org.wso2.siddhi.extension.RSSReader;

import net.htmlparser.jericho.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.io.IOException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.jsoup.*;
import org.jsoup.select.*;

public class FullTextReader extends FunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(FullTextReader.class);
    // private static RSSReader instance = null;

    private URL rssURL;
    private String[] Titles;

    @Override
    public Type getReturnType() {
        // TODO Auto-generated method stub
        return Attribute.Type.STRING;
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
        if (attributeExpressionExecutors.length != 1) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to math:sin() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }

    }

    @Override
    protected Object execute(Object[] data) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Object execute(Object data) {
        // TODO Auto-generated method stub
        if (data != null) {
            // type-conversion
            if (((String) data).contains("https:"))
                try {
                    setURL(new URL((String) data));
                    String[] Links = writeFeed();
                    String TextField = "";
                    for (int i = 0; i < Links.length; i++) {
                        TextField = TextField.concat("#Artical:" + String.valueOf((i + 1)) + "#");
                        TextField = TextField.concat("\n");
                        TextField = TextField.concat(Titles[i]);
                        TextField = TextField.concat("\n");
                        TextField = TextField.concat(ReadText(Links[i]));
                        TextField = TextField.concat("\n\n");
                    }
                    return TextField;
                } catch (Exception e) {
                    // e.printStackTrace();
                    logger.error("Error in read full text: " + e.getMessage());
                }
            else {
                throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function should contain URL");
            }
        } else {
            throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function cannot be null");
        }
        return null;
    }

    public String ReadText(String Link) {
        String text = "";
        try {
            String url = Link;
            org.jsoup.nodes.Document doc = (org.jsoup.nodes.Document) Jsoup.connect(url).get();
            Elements paragraphs = doc.select("p");
            for (org.jsoup.nodes.Element p : paragraphs) {
                text = text.concat(p.text());
            }

        } catch (IOException ex) {
            logger.error("Error in read full text connection: " + ex.getMessage());
        }

        return text;

    }

    public String clr(String htl) {
        Source source = new Source(htl);
        return source.getTextExtractor().toString();
    }

    public void setURL(URL url) {
        rssURL = url;
    }

    public String[] writeFeed() {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(rssURL.openStream());
            NodeList items = doc.getElementsByTagName("item");
            String[] Links = new String[items.getLength()];
            Titles = new String[items.getLength()];
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                Links[i] = (String) getValue(item, "link");
                Titles[i] = (String) getValue(item, "title");
            }
            return Links;
        } catch (Exception e) {
            logger.error("Error in read full text Details: " + e.getMessage());
        }
        return null;
    }

    public String getValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

}
