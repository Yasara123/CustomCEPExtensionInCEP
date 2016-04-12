package org.wso2.siddhi.extension.ClearSentiments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentRateStanford extends FunctionExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SentimentRateStanford.class);
    @Override
    public Type getReturnType() {
        // TODO Auto-generated method stub
        return Type.INT;
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
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to ClearSentiment:Stanford() function, " + "required 2, but found "
                            + attributeExpressionExecutors.length);
        }
        Attribute.Type attributeType1 = attributeExpressionExecutors[0].getReturnType();
        if (!(attributeType1 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument 1 of ClearSentiment:Stanford() function");
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
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        // gender,lemma,ner,parse,pos,sentiment,sspplit, tokenize
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // read some text in the text variable
        int totRate=0;
        String sentimentText = (String) data;
        String[] ratings = { "Very Negative", "Negative", "Neutral", "Positive",
                "Very Positive" };
        Annotation annotation = pipeline.process(sentimentText);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentAnnotatedTree.class);
            int score = RNNCoreAnnotations.getPredictedClass(tree);
            totRate=totRate+score;
          
        }
        return totRate;
    }

}
