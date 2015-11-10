package ru.atc.camel.opsm.events;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.cache.CacheConstants;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarmSeverity;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarm;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;

//import ru.at_consulting.itsm.device.Device;
import ru.at_consulting.itsm.event.Event;
import ru.atc.camel.opsm.events.api.OVMMDevices;
import ru.atc.camel.opsm.events.api.OVMMEvents;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

//import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Driver;



public class OPSMConsumer extends ScheduledPollConsumer {
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public OPSMEndpoint endpoint;
	
	public String di_status_col =  "attribute009";
	public String do_status_col =  "attribute004";
		
	public static ModelCamelContext context;
	
	public enum PersistentEventSeverity {
	    OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;
		
	    public String value() {
	        return name();
	    }

	    public static PersistentEventSeverity fromValue(String v) {
	        return valueOf(v);
	    }
	}

	public OPSMConsumer(OPSMEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        //this.afterPoll();
        this.setDelay(endpoint.getConfiguration().getDelay());
	}
	
	public static ModelCamelContext getContext() {
		// TODO Auto-generated method stub
				return context;
	}
	
	public static void setContext(ModelCamelContext context1){
		context = context1;

	}

	@Override
	protected int poll() throws Exception {
		
		String operationPath = endpoint.getOperationPath();
		
		if (operationPath.equals("events")) return processSearchEvents();
		
		// only one operation implemented for now !
		throw new IllegalArgumentException("Incorrect operation: " + operationPath);
	}
	
	// "throws Exception" 
	private int processSearchEvents() {
		
		//Long timestamp;
		DataSource dataSource = setupDataSource();
		
		List<HashMap<String, Object>> listDIsAndTable = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String, Object>> listDIsStatuses = null;
		int events = 0;
		int statuses = 0;
		try {
			
			logger.info( String.format("***Try to get DIs***"));
			
			listDIsAndTable = getDIsAndTable(dataSource);
			
			logger.info( String.format("***Received %d DIs from SQL***", listDIsAndTable.size()));
			String dititle, ditable;
			
			logger.info( String.format("***Try to get DIs statuses***"));
			for(int i=0; i < listDIsAndTable.size(); i++) {
			  	
				dititle = listDIsAndTable.get(i).get("title").toString();
				ditable  = listDIsAndTable.get(i).get("tablename").toString();
				logger.debug("MYSQL row " + i + ": " + dititle + 
						" " + ditable);
				
				listDIsStatuses = getDIsStatuses(dititle, ditable, dataSource);
				
				if ( listDIsStatuses != null ){
					HashMap<String, Object> DI = listDIsStatuses.get(0);
					
					List<Event> dievents = genEvents(dititle, DI);
					
					for(int i1=0; i1 < dievents.size(); i1++) {
						
						statuses++;
						
						logger.debug("*** Create Exchange ***" );
						
						String key = dievents.get(i1).getHost() + "_" +
								dievents.get(i1).getObject() + "_" + dievents.get(i1).getParametrValue();
						String key1 = dievents.get(i1).getHost() + "_" +
								dievents.get(i1).getObject();
						
						Exchange exchange = getEndpoint().createExchange();
						exchange.getIn().setBody(dievents.get(i1), Event.class);
						exchange.getIn().setHeader("EventUniqId", key);
						
						exchange.getIn().setHeader("EventUniqIdWithoutStatus", key1);
						
						exchange.getIn().setHeader("EventStatus", dievents.get(i1).getParametrValue());
						
						exchange.getIn().setHeader("Object", dievents.get(i1).getObject());
						exchange.getIn().setHeader("Timestamp", dievents.get(i1).getTimestamp());
						
						exchange.getIn().setHeader(CacheConstants.CACHE_OPERATION, CacheConstants.CACHE_OPERATION_CHECK);
						exchange.getIn().setHeader(CacheConstants.CACHE_KEY, key);
						
						exchange.getIn().setHeader("CamelCacheOperation1", "CamelCacheCheck");
						exchange.getIn().setHeader("CamelCacheKey1", key);
						
						logger.debug(String.format("*** CACHE HEADERS: %s %s  ***", CacheConstants.CACHE_OPERATION, CacheConstants.CACHE_OPERATION_CHECK ));
						logger.debug(String.format("*** CACHE HEADERS: %s %s  ***", CacheConstants.CACHE_KEY, key ));
						
						exchange.getIn().setHeader("TEST", key);
						//exchange.getIn().setHeader("DeviceType", vmevents.get(i).getDeviceType());
						
						
	
						try {
							getProcessor().process(exchange);
							events++;
							
							//File cachefile = new File("sendedEvents.dat");
							//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							logger.error( String.format("Error while process Exchange message: %s ", e));
						} 
						
						/*
						logger.debug("*** Create Exchange for DELETE ***" );
						
						
						context = getContext();
						
						Endpoint endpoint = context.getEndpoint("cache://ServerCacheTest");
						logger.debug("*** endpoint ***" + endpoint );
						Exchange exchange1 = endpoint.createExchange();
						logger.debug("*** exchange1 ***" + exchange1 );
					    //exchange.getIn().setBody(vmevents.get(i1), Event.class);
					    exchange1.getIn().setHeader(CacheConstants.CACHE_OPERATION, CacheConstants.CACHE_OPERATION_DELETE);
					    exchange1.getIn().setHeader(CacheConstants.CACHE_KEY, key1+"_OK");
					    //exchange1.getIn().setHeader("EventId", event.getExternalid());

					    
					    Producer producer2 = null;
						try {
							producer2 = endpoint.createProducer();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					    try {
					    	producer2.process(exchange1);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						*/
					
					}
				}
				//genevent = geEventObj( device, "fcFabric" );
				
				/*
				logger.info("Create Exchange container");
				Exchange exchange = getEndpoint().createExchange();
				exchange.getIn().setBody(listFinal.get(i), Device.class);
				exchange.getIn().setHeader("DeviceId", listFinal.get(i).getId());
				exchange.getIn().setHeader("DeviceType", listFinal.get(i).getDeviceType());
				
				

				try {
					getProcessor().process(exchange);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				*/
	  	
			}
			
						
			logger.info( String.format("***Received %d VMs statuses from SQL*** ", statuses));
	  
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get VM SQL: %s ", e));
		}
		
		logger.info( String.format("***Sended to Exchange messages: %d ***", events));
		
		removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
	
        return 1;
	}
	
	private static List<Event> genEvents( String dititle, HashMap<String, Object> DIStatuses ) {
		
		String datetime = DIStatuses.get("datetime").toString();
		String status_colour = DIStatuses.get("di_status_colour").toString();
		String status_value = DIStatuses.get("di_status_value").toString();
				//String ping_colour1 = listVmStatuses.get(0).get("ping_colour").toString();
		//vmuuid  = listVmStatuses.get(0).get("uuid").toString();
		logger.debug(dititle + ": " + datetime );
		logger.debug(dititle + ": " + status_colour);
		logger.debug(dititle + ": " + status_value);

		Event event;
		List<Event> eventList = new ArrayList<Event>();
		// Create Event object for further use
		event = genDeviceObj(dititle, "status", 
				status_colour, status_value, DIStatuses.get("datetime").toString());
		eventList.add(event);
		/*
		 event = genDeviceObj(vmtitle, "ping", 
				"#FF0000", vmStatuses.get("datetime").toString());
		eventList.add(event);
		*/
		
		/*
		event = genDeviceObj(dititle, "snmp", 
				vmStatuses.get("snmp_colour").toString(), vmStatuses.get("datetime").toString());
		eventList.add(event);
		*/

		return eventList;
	}

	private static Event genDeviceObj( String vmtitle, String object, String parametrColour, String parametrValue, String datetime ) {
		Event event;
		
		event = new Event();
		//Timestamp timestamp = null;
		long timeInMillisSinceEpoch = 0;
		//long timeInMinutesSinceEpoch = 0;
		//DATE FORMAT: 2015-11-02 17:55:33.0
		String eventdate = datetime;
		try {
		    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		    Date parsedDate = dateFormat.parse(eventdate);
		    timeInMillisSinceEpoch = parsedDate.getTime() / 1000; 
		    //timeInMinutesSinceEpoch = timeInMillisSinceEpoch / (60 * 1000);
		} catch(Exception e) {
			//this generic but you can control another types of exception
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (timeInMillisSinceEpoch != 0){
			logger.debug("timeInMillisSinceEpoch: " + timeInMillisSinceEpoch);
			//logger.info("timeInMinutesSinceEpoch: " + timeInMinutesSinceEpoch);
			event.setTimestamp(timeInMillisSinceEpoch);
		}
		
		String status;
		
		event.setHost(vmtitle);
		event.setCi(vmtitle);
		//vmStatuses.get("ping_colour").toString())
		event.setObject(object);
		event.setParametr("Status");
		status = setRightValue(parametrValue);
		//status = parametrValue;
		event.setParametrValue(status);
		event.setSeverity(setRightSeverity(parametrColour));
		event.setMessage(setRightMessage(vmtitle, object, status));
		event.setCategory("HARDWARE");
		event.setStatus("OPEN");
		event.setService("OPSM");
		

		//System.out.println(event.toString());
		
		logger.debug(event.toString());
		
		return event;
				
	}
	
	
	
	private static String setRightMessage(String dititle, String object, String status) {
		// TODO Auto-generated method stub
		
		String newmessage = String.format("DI: %s. Статус контакта: %s %s", dititle, object, status);
		return newmessage;
	}

	private List<HashMap<String, Object>> getDIsStatuses(String dititle, String ditable, DataSource dataSource) throws SQLException {
		// TODO Auto-generated method stub
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
        Connection con = null; 
        PreparedStatement pstmt;
        ResultSet resultset = null;
        try {
        	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
        	//String table_prefix =  endpoint.getConfiguration().getTable_prefix();
        	
        	//String vmtable = ditable;

        	
            pstmt = con.prepareStatement(String.format("SELECT datetime, "
            			+ " %s_colour as di_status_colour, %s_value as di_status_value "
            			+ "FROM `%s`" , di_status_col, di_status_col, ditable ));
            //pstmt.setString(1, vm_vmtools_col);
            
            logger.debug("MYSQL query: " +  pstmt.toString()); 
            resultset = pstmt.executeQuery();
            //con.commit();
            list = convertRStoList(resultset);
            
            //list.get(0).get(ping_colour);
            
            resultset.close();
            pstmt.close();
            con.close();
            return list;
            
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//logger.error( "Error1 while SQL execution: " + e.getMessage() );
			//logger.error( "Error2 while SQL execution: " + e.getCause() );
			logger.error( String.format("Error while SQL execution: %s ", e));
			//logger.error( ExceptionUtils.getFullStackTrace(e) );
			 if (con != null) con.close();
			//logger.error("Error while SQL executiom: " + e.printStackTrace());
			
			return null;

		} finally {
            if (con != null) con.close();
        }
		
	}

	private List<HashMap<String, Object>> getDIsAndTable(DataSource dataSource) throws SQLException {
		// TODO Auto-generated method stub
        
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
        Connection con = null; 
        PreparedStatement pstmt;
        ResultSet resultset = null;
        try {
        	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
            pstmt = con.prepareStatement("SELECT tree_DI.title, tree_DI.type, tree_DI.tablename " +
                        "FROM tree_DI " +
                        "WHERE tree_DI.show = ?"
                        + " AND tree_DI.tablename <> ?");
                       // +" LIMIT ?;");
            pstmt.setInt(1, 1);
            pstmt.setString(2, "");
            
            logger.debug("MYSQL query: " +  pstmt.toString()); 
            resultset = pstmt.executeQuery();
            //con.commit();
            
            list = convertRStoList(resultset);
            
            
            resultset.close();
            pstmt.close();
            
            con.close();
            
            return list;
            
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			return null;

		} finally {
            if (con != null) con.close();
            
            //return list;
        }
		
	}
	
	private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {
		
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
		try {
			ResultSetMetaData md = resultset.getMetaData();
	        int columns = md.getColumnCount();
	        //result.getArray(columnIndex)
	        //resultset.get
	        logger.debug("MYSQL columns count: " + columns); 
	        
	        resultset.last();
	        int count = resultset.getRow();
	        logger.debug("MYSQL rows2 count: " + count); 
	        resultset.beforeFirst();
	        
	        int i = 0, n = 0;
	        //ArrayList<String> arrayList = new ArrayList<String>(); 
	
	        while (resultset.next()) {              
	        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
	            for(int i1=1; i1<=columns; ++i1) {
	                row.put(md.getColumnLabel(i1),resultset.getObject(i1));
	            }
	            list.add(row);                 
	        }
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return null;

		} finally {

		}
	}

	private DataSource setupDataSource() {
		
		String url = String.format("jdbc:mysql://%s:%s/%s",
		endpoint.getConfiguration().getMysql_host(), endpoint.getConfiguration().getMysql_port(),
		endpoint.getConfiguration().getMysql_db());
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername( endpoint.getConfiguration().getUsername() );
        ds.setPassword( endpoint.getConfiguration().getPassword() );
        ds.setUrl(url);
              
        return ds;
    }
	
	public static String setRightValue(String statusValue)
	{
		String newstatus = "";
		
		switch (statusValue) {
        	case "1":  newstatus = "ON";break;
        	case "0":  newstatus = "OFF";break;
        	default: newstatus = "NA";break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** status: " + newstatus);
		*/
		return newstatus;
	}
	
	public static String setRightSeverity(String colour)
	{
		String newseverity = "";
		/*
		 * 
		<pre>
 * &lt;simpleType name="ISXCAlarmSeverity">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="ERROR"/>
 *     &lt;enumeration value="FAILURE"/>
 *     &lt;enumeration value="CRITICAL"/>
 *     &lt;enumeration value="WARNING"/>
 *     &lt;enumeration value="INFORMATIONAL"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>

		 */
		
		
		
		switch (colour) {
        	case "#006600":  newseverity = PersistentEventSeverity.OK.name();break;
        	case "#FF0000":  newseverity = PersistentEventSeverity.CRITICAL.name();break;
        	default: newseverity = PersistentEventSeverity.INFO.name();break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** newseverity: " + newseverity);
		*/
		return newseverity;
	}
	
	public void removeLineFromFile(String file, String lineToRemove) {
		BufferedReader br = null;
		PrintWriter pw = null;
	    try {

	      File inFile = new File(file);

	      if (!inFile.isFile()) {
	        System.out.println("Parameter is not an existing file");
	        return;
	      }

	      //Construct the new file that will later be renamed to the original filename.
	      File tempFile = new File(inFile.getAbsolutePath() + ".tmp");

	      br = new BufferedReader(new FileReader(file));
	      pw = new PrintWriter(new FileWriter(tempFile));

	      String line = null;

	      //Read from the original file and write to the new
	      //unless content matches data to be removed.
	      while ((line = br.readLine()) != null) {

	        if (!line.trim().equals(lineToRemove)) {

	          pw.println(line);
	          pw.flush();
	        }
	      }
	      pw.close();
	      br.close();

	      //Delete the original file
	      if (!inFile.delete()) {
	        System.out.println("Could not delete file");
	        return;
	      }

	      //Rename the new file to the filename the original file had.
	      if (!tempFile.renameTo(inFile))
	        System.out.println("Could not rename file");

	    }
	    catch (FileNotFoundException ex) {
	      ex.printStackTrace();
	    }
	    catch (IOException ex) {
	      ex.printStackTrace();
	    }
	    finally {
	    	try {
	    		pw.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	  }

}