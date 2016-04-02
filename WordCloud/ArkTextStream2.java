package org.wso2.siddhi.extension.WorldCloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
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

import cmu.arktweetnlp.Tagger;
import cmu.arktweetnlp.Tagger.TaggedToken;

public class ArkTextStream2 extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ArkTextStream2.class);
    public static String[] stopWord = { "trump", "donaldtrump", "realdonaldtrump", "via", "wow", "10", "b4", "gt",
            "get", "gets", "come", "go", "ben", "carson", "rubio", "bencarson", "cruzcrew", "feelthebern", "sanders",
            "voteTrump", "clinton", "cruz", "tedcruz", "bernie", "berniesanders", "makeamericagreatagain",
            "trumptrain", "donald", "one", "two", "new", "man", "rt", "i", "me", "my", "myself", "we", "us", "our",
            "just", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his",
            "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs",
            "themselves", "what", "which", "who", "whom", "whose", "this", "that", "these", "those", "am", "is", "are",
            "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "will",
            "would", "should", "can", "could", "ought", "m", "you", "re", "he", "s", "she's", "it's", "we're",
            "they're", "i've", "you've", "we've", "ve", "s", "d", "ll", "you'll", "he'll", "she'll", "we'll",
            "they'll", "isn't", "aren't", "wasn", "weren", "hasn", "haven", "hadn", "doesn", "don", "didn", "won",
            "wouldn", "shan", "shouldn", "can", "t", "cannot", "couldn", "mustn", "let", "that", "who", "what", "here",
            "there", "when", "where", "why", "how's", "a", "an", "the", "and", "but", "if", "or", "because", "as",
            "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
            "during", "before", "after", "above", "below", "to", "from", "up", "upon", "down", "in", "out", "on",
            "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
            "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
            "own", "same", "so", "than", "too", "very", "say", "says", "said", "shall", "trump", "donaldtrump",
            "hillary", "clinton", "hillaryclinton", "ted", "cruz", "tedcruz", "rick", "santorum", "ricksantorum",
            "marco", "rubio", "marcorubio", "mike", "huckabee", "mikehuckabee", "martin", "omalley", "martinomalley",
            "carly", "fiorina", "carlyfiorina", "rand", "paul", "randpaul", "john", "kasich", "johnkasich", "ben",
            "carson", "bencarson", "lindsley", "graham", "lindsleygraham", "scott", "walker", "scottwalker", "jim",
            "gilmore", "jimgilmore", "jeb", "bush", "jebbush", "http", "https", "chris", "christie", "chrischristie",
            "pataki", "george", "georgepataki", "election", "election2016", "wso2", "wso2con", "wso2conasia", "2016",
            "wso2con2016" };
    public static HashSet<String> hs;
    String modelFilename = "/home/ubuntu/model.20120919";
    Tagger tagger;

    // ArrayList<String> newList;
    // public static HashMap hm;
    private VariableExpressionExecutor variableExpressionText;
    private String op = "NLP";

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
        StreamEvent streamEvent;
        long lStartTime = System.currentTimeMillis();
        // TODO Auto-generated method stub
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            String Processed = "";
            Processed = (String) variableExpressionText.execute(streamEvent);
            String SendOut = "";
            if (op.equalsIgnoreCase("NLP")) {
                SendOut = funNLP(Processed);
                // SendOut = funCommon(SendOut);
            } else if (op.equalsIgnoreCase("COMMON")) {
                SendOut = funCommon(Processed);
            }
            streamEvent = setAttributeText(streamEvent, SendOut);
            returnEventChunk.add(streamEvent);
        }
        long lEndTime = System.currentTimeMillis();
        long difference = lEndTime - lStartTime;
        logger.info("Stop Words: " + difference);
        nextProcessor.process(returnEventChunk);
    }
    /*===========If the Option is Use NLP=======================================*/
    private String funNLP(String Processed) {
        String SendOut = "";
        List<TaggedToken> taggedTokens = tagger.tokenizeAndTag(Processed);
        for (TaggedToken token : taggedTokens) {
            if (token.tag.contains("N") || token.tag.contains("A")) {
                SendOut = SendOut.concat(token.token.concat(" "));
            }
        }
        return SendOut;
    }
/*===========If the Option is Common=======================================*/
    private String funCommon(String source) {
        String newString = " ";
        String reg = "[0-9]+";
        source = source.replaceAll("#[A-Za-z0-9]*", " ");
        String regex = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        source = source.replaceAll(regex, " ");
        source = source.replaceAll("@[A-Za-z0-9]*", " ");
        source = source.toLowerCase();
        source = source.replaceAll("[^a-z]", " ");
        source = source.replaceAll(reg, " ");
        List<String> stringList = new ArrayList<String>(Arrays.asList(source.split(" ")));
        stringList.removeAll(hs);
        newString = stringList.toString().replaceAll(",", "");
        if (newString.length() > 2)
            return newString.substring(1, newString.length() - 2);
        else
            return "";
    }

    private StreamEvent setAttributeText(StreamEvent event, String val) {
        switch (variableExpressionText.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionText.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionText.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionText.getPosition()[3]);
            break;
        }
        return event;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 2)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionText = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a String NLP or COMMON, but found a other parameter");
        } else {
            if (attributeExpressionExecutors[1].execute(null).toString().equalsIgnoreCase("NLP")
                    || attributeExpressionExecutors[1].execute(null).toString().equalsIgnoreCase("COMMON")) {
                op = (String) attributeExpressionExecutors[1].execute(null);
            } else {
                throw new UnsupportedOperationException("Invalid Option Selection. Option shoud be  NLP or COMMON ");
            }
        }
        if (op.equalsIgnoreCase("NLP")) {
            tagger = new Tagger();
            try {
                tagger.loadModel(modelFilename);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } else {
            hs = new HashSet<String>();
            int len = stopWord.length;
            for (int i = 0; i < len; i++) {
                hs.add(stopWord[i]);
            }
            /*
             * stringList=new ArrayList<String>(Arrays.asList(stopWord));
             * newList=new ArrayList<String>();
             * hm=new HashMap<String, Integer>();
             */
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        return attributeList;
    }

}
