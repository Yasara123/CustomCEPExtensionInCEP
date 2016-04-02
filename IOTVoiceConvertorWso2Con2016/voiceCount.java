package org.wso2.siddhi.extension.TwitterSentiment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

public class voiceCount extends StreamProcessor {
    VariableExpressionExecutor variableExpressionURLName;

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
        // TODO Auto-generated method stub
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        String name;
        Database database = new Database();
        Connection connection = null;
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            connection = database.Get_Connection();
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            
        }
        int Cassandra = 0, Hadoop = 0, MapReduce = 0, NLP = 0, Predictive = 0, Batch = 0, Realtimeanalytics = 0, Spark = 0, machinelearner = 0, ML = 0, temporal = 0, timeseries = 0, Apps = 0, DevOps = 0, Native = 0, Waterfall = 0, Agile = 0, SaaS = 0, Amazon = 0, Azure = 0, PaaS = 0, SOA = 0, orchestration = 0, mediation = 0, ESB = 0, MS4J = 0, MSS = 0, rules = 0, management = 0, policies = 0, policy = 0, SSO = 0, singlesignon = 0, users = 0, profiles = 0, Codegen = 0, car = 0, conference = 0;
        int IoT = 0, BigData = 0, Analytics = 0, API = 0, AppDev = 0, Cloud = 0, Integration = 0, Microservices = 0, Governance = 0, Identity = 0, Vega = 0, WSO2Con = 0;
        // boolean firstEle=false;
        while (streamEventChunk.hasNext()) {
            // System.out.println("Inside the function-------------------------------------------------------------");
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            name = (String) variableExpressionURLName.execute(streamEvent);
            try {

                java.sql.PreparedStatement ps1 = connection
                        .prepareStatement("select catogory,count from Voice3 where name='" + name + "'");
                java.sql.ResultSet rs1 = ps1.executeQuery();
                while (rs1.next()) {

                    if ("IoT".equalsIgnoreCase(rs1.getString("catogory")))
                        IoT = rs1.getInt("count");
                    if ("Analytics".equalsIgnoreCase(rs1.getString("catogory"))||"Cassandra".equalsIgnoreCase(rs1.getString("catogory"))||"Hadoop".equalsIgnoreCase(rs1.getString("catogory"))||"MapReduce".equalsIgnoreCase(rs1.getString("catogory"))||"NLP".equalsIgnoreCase(rs1.getString("catogory"))||"Predictive".equalsIgnoreCase(rs1.getString("catogory"))||"Batch".equalsIgnoreCase(rs1.getString("catogory"))||"Real-time analytics".equalsIgnoreCase(rs1.getString("catogory"))||"Spark".equalsIgnoreCase(rs1.getString("catogory"))||"machine learner".equalsIgnoreCase(rs1.getString("catogory"))||"ML".equalsIgnoreCase(rs1.getString("catogory"))||"temporal".equalsIgnoreCase(rs1.getString("catogory"))||"timeseries".equalsIgnoreCase(rs1.getString("catogory")))
                        Analytics = rs1.getInt("count");
                    if ("API".equalsIgnoreCase(rs1.getString("catogory")))
                        API = rs1.getInt("count");
                    if ("AppDev".equalsIgnoreCase(rs1.getString("catogory"))||"Apps".equalsIgnoreCase(rs1.getString("catogory"))||"DevOps".equalsIgnoreCase(rs1.getString("catogory"))||"Native".equalsIgnoreCase(rs1.getString("catogory"))||"Waterfall".equalsIgnoreCase(rs1.getString("catogory"))||"Agile".equalsIgnoreCase(rs1.getString("catogory"))||"SaaS".equalsIgnoreCase(rs1.getString("catogory")))
                        AppDev = rs1.getInt("count");
                    if ("Cloud".equalsIgnoreCase(rs1.getString("catogory"))||"Amazon".equalsIgnoreCase(rs1.getString("catogory"))||"Azure".equalsIgnoreCase(rs1.getString("catogory"))||"PaaS".equalsIgnoreCase(rs1.getString("catogory")))
                        Cloud = rs1.getInt("count");
                    if ("Integration".equalsIgnoreCase(rs1.getString("catogory"))||"SOA".equalsIgnoreCase(rs1.getString("catogory"))||"orchestration".equalsIgnoreCase(rs1.getString("catogory"))||"mediation".equalsIgnoreCase(rs1.getString("catogory"))||"ESB".equalsIgnoreCase(rs1.getString("catogory")))                  
                       Integration = rs1.getInt("count");
                    if ("Microservices".equalsIgnoreCase(rs1.getString("catogory"))|| "MS4J".equalsIgnoreCase(rs1.getString("catogory"))||"MSS".equalsIgnoreCase(rs1.getString("catogory")))
                        Microservices = rs1.getInt("count");
                    if ("Identity".equalsIgnoreCase(rs1.getString("catogory"))||"Governance".equalsIgnoreCase(rs1.getString("catogory"))|| "rules".equalsIgnoreCase(rs1.getString("catogory"))||"management".equalsIgnoreCase(rs1.getString("catogory"))||"policies".equalsIgnoreCase(rs1.getString("catogory"))||"policy".equalsIgnoreCase(rs1.getString("catogory")))
                        Governance = rs1.getInt("count");
                    if ("Identity".equalsIgnoreCase(rs1.getString("catogory"))||"SSO".equalsIgnoreCase(rs1.getString("catogory"))||"single-sign on".equalsIgnoreCase(rs1.getString("catogory"))||"users".equalsIgnoreCase(rs1.getString("catogory"))||"profiles".equalsIgnoreCase(rs1.getString("catogory")))
                        Identity = rs1.getInt("count");
                    if ("Vega".equalsIgnoreCase(rs1.getString("catogory"))||"Codegen".equalsIgnoreCase(rs1.getString("catogory"))||"car".equalsIgnoreCase(rs1.getString("catogory")))
                        Vega = rs1.getInt("count");
                    if ("WSO2".equalsIgnoreCase(rs1.getString("catogory"))||"WSO2Con".equalsIgnoreCase(rs1.getString("catogory"))||"conference".equalsIgnoreCase(rs1.getString("catogory")))
                        WSO2Con = rs1.getInt("count");


                }
                connection.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                
            }
            java.sql.PreparedStatement ps2;
            String startTm;
            String place = "";
            String dis = "";
            String nm = "";
            String pic = "";
            String T = "";
            long min = 0;
            try {
                ps2 = connection.prepareStatement("select started_time,pic,started_time,place,name,discription from sessionData where speaker='" + name
                        + "'");
                java.sql.ResultSet rs2 = ps2.executeQuery();
                place = rs2.getString("place");
                startTm = rs2.getString("started_time");
                dis = rs2.getString("dis");
                nm = rs2.getString("name");
                pic = rs2.getString("pic");
                T = rs2.getString("title");
                Date d1 = null;
                Date d2 = null;

                try {
                    d1 = date;
                    d2 = format.parse(startTm);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                long diff = d1.getTime() - d2.getTime();// as given

                min = TimeUnit.MILLISECONDS.toMinutes(diff);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
               
            }
            complexEventPopulater.populateComplexEvent(streamEvent, new Object[] { IoT, API, Cloud, Microservices, Integration, Vega,
                    AppDev, Analytics, Governance, Identity, WSO2Con,  min, place,nm,dis,T,pic});
            returnEventChunk.add(streamEvent);
            nextProcessor.process(returnEventChunk);
        }

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 1)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionURLName = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        // TRUMP,CLINTON,BERNIE,BEN,MALLEY,BUSH,CRUZ,CHRIS,FIORINA,GILMORE,GRAHAM,HUCKABEE,JOHN,GEORGE,RAND,RUBIE,RICK,WALKER
        attributeList.add(new Attribute("IOT", Attribute.Type.INT));
        attributeList.add(new Attribute("API", Attribute.Type.INT));
        attributeList.add(new Attribute("Cloud", Attribute.Type.INT));
        attributeList.add(new Attribute("Microservices", Attribute.Type.INT));
        attributeList.add(new Attribute("Integration", Attribute.Type.INT));
        attributeList.add(new Attribute("Vega", Attribute.Type.INT));
        attributeList.add(new Attribute("AppDev", Attribute.Type.INT));
        attributeList.add(new Attribute("Analytics", Attribute.Type.INT));
        attributeList.add(new Attribute("Governance", Attribute.Type.INT));
        attributeList.add(new Attribute("Identity", Attribute.Type.INT));
        attributeList.add(new Attribute("WSO2Con", Attribute.Type.INT));
        attributeList.add(new Attribute("Time", Attribute.Type.LONG));
        attributeList.add(new Attribute("Place", Attribute.Type.STRING));
        attributeList.add(new Attribute("Name", Attribute.Type.STRING));
        attributeList.add(new Attribute("Dis", Attribute.Type.STRING));
        attributeList.add(new Attribute("Title", Attribute.Type.STRING));
        attributeList.add(new Attribute("Pic", Attribute.Type.STRING));
        return attributeList;

    }

}

class Database {

    public Connection Get_Connection() throws Exception {
        try {
            String connectionURL = "jdbc:mysql://localhost:3306/wso2conCEP";
            Connection connection = null;
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            connection = DriverManager.getConnection(connectionURL, "root", "root");
            return connection;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        }
    }

}
