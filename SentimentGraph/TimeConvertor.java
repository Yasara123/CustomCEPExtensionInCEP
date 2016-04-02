package org.wso2.siddhi.extension.CustomWin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
/*getMax:currentTimeCustForm("yyyy/MM/dd")*/
public class TimeConvertor extends FunctionExecutor{
    private String date_format ;
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
        if (!(attributeExpressionExecutors.length==1)) {
            throw new UnsupportedOperationException(
                    "Invalid number of Arguments");
        } 
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
             date_format = ((String) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an Varible String");
        }
        
    }

    @Override
    protected Object execute(Object[] data) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Object execute(Object data) {

            Date date = new Date();
            SimpleDateFormat dt1 = new SimpleDateFormat(date_format);
            return dt1.format(date);
            
      
    }

}
