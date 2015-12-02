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
import java.util.concurrent.TimeUnit;
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
import com.mysql.jdbc.SQLError;



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
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
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
	
	@Override
	public long beforePoll(long timeout) throws Exception {
		
		logger.info("*** Before Poll!!!");
		// only one operation implemented for now !
		//throw new IllegalArgumentException("Incorrect operation: ");
		
		//send HEARTBEAT
		genHeartbeatMessage(getEndpoint().createExchange());
		
		return timeout;
	}
	
	private void genErrorMessage(String message) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage(textError + message);
		genevent.setEventCategory("ADAPTER");
		genevent.setSeverity(PersistentEventSeverity.CRITICAL.name());
		genevent.setTimestamp(timestamp);
		genevent.setEventsource("OPSM_EVENTS_ADAPTER");
		
		logger.info(" **** Create Exchange for Error Message container");
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("EventIdAndStatus", "Error_" +timestamp);
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Events");
        exchange.getIn().setHeader("Type", "Heartbeat");

        try {
			getProcessor().process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

	
	public static void genHeartbeatMessage(Exchange exchange) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		//String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage("Сигнал HEARTBEAT от адаптера");
		genevent.setEventCategory("ADAPTER");
		genevent.setObject("HEARTBEAT");
		genevent.setSeverity(PersistentEventSeverity.OK.name());
		genevent.setTimestamp(timestamp);
		genevent.setEventsource("OPSM_EVENT_ADAPTER");
		
		logger.info(" **** Create Exchange for Heartbeat Message container");
        //Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Events");

        try {
        	//Processor processor = getProcessor();
        	//.process(exchange);
        	//processor.process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} 
	}
	
	
	// "throws Exception" 
	private int processSearchEvents()  throws Exception, Error, SQLException {
		
		//Long timestamp;
		BasicDataSource dataSource = setupDataSource();
		
		List<HashMap<String, Object>> listDIsAndTable = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String, Object>> listIOsStatuses = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String, Object>> listDOsAndTable = new ArrayList<HashMap<String,Object>>();
		//List<HashMap<String, Object>> listDOsStatuses = null;
		int events = 0;
		int statuses = 0;
		try {
			
			// DI
			logger.info( String.format("***Try to get DIs***"));
			listDIsAndTable = getIOsAndTable(dataSource, "DI");
			logger.info( String.format("***Received %d DIs from SQL***", listDIsAndTable.size()));
			
			// DO
			logger.info( String.format("***Try to get DOs***"));
			listDOsAndTable = getIOsAndTable(dataSource, "DO");
			logger.info( String.format("***Received %d DOs from SQL***", listDOsAndTable.size()));
			
			List<HashMap<String, Object>> listIOsAndTable = new ArrayList<HashMap<String,Object>>();
			listIOsAndTable.addAll(listDIsAndTable);
			listIOsAndTable.addAll(listDOsAndTable);
			
			logger.info( String.format("***Received %d total IOs from SQL***", listIOsAndTable.size()));
			
			String iotitle, iotable;
			
			int ioid;
			for(int i=0; i < listDIsAndTable.size(); i++) {
				
				logger.info( String.format("***Try to get DIs statuses***"));
			  	
				iotitle = listDIsAndTable.get(i).get("title").toString();
				iotable  = listDIsAndTable.get(i).get("tablename").toString();
				ioid  = Integer.parseInt(listDIsAndTable.get(i).get("ioid").toString());
				
				logger.debug("MYSQL row " + i + ": " + iotitle + 
						" " + iotable);
				
				listIOsStatuses.addAll(getIOsStatuses(iotitle, iotable, "DI", ioid, dataSource));
			}
			
			for(int i=0; i < listDOsAndTable.size(); i++) {
				
				logger.info( String.format("***Try to get DOs statuses***"));
			  	
				iotitle = listDOsAndTable.get(i).get("title").toString();
				iotable  = listDOsAndTable.get(i).get("tablename").toString();
				ioid  = Integer.parseInt(listDOsAndTable.get(i).get("ioid").toString());
				logger.debug("MYSQL row " + i + ": " + iotitle + 
						" " + iotable);
				
				listIOsStatuses.addAll(getIOsStatuses(iotitle, iotable, "DO", ioid, dataSource));
			}
			
			String iotype;
			//if ( listIOsStatuses == null )
			//		return 0;
			if ( listIOsStatuses != null ){
				for(int i=0; i < listIOsStatuses.size(); i++) {
				
					HashMap<String, Object> io = listIOsStatuses.get(i);
					iotype = listIOsStatuses.get(i).get("iotype").toString();
					iotitle = listIOsStatuses.get(i).get("iotitle").toString();
					
					List<Event> ioevents = genEvents(iotitle, io);
					
					for(int i1=0; i1 < ioevents.size(); i1++) {
						
						statuses++;
						
						logger.debug("*** Create Exchange ***" );
						
						String key = ioevents.get(i1).getHost() + "_" + iotype + "_" +
								ioevents.get(i1).getObject() + "_" + ioevents.get(i1).getParametrValue();
						String key1 = ioevents.get(i1).getHost() + "_" + iotype + "_" +
								ioevents.get(i1).getObject();
						
						Exchange exchange = getEndpoint().createExchange();
						exchange.getIn().setBody(ioevents.get(i1), Event.class);
						exchange.getIn().setHeader("EventUniqId", key);
						
						exchange.getIn().setHeader("EventUniqIdWithoutStatus", key1);
						
						exchange.getIn().setHeader("EventStatus", ioevents.get(i1).getParametrValue());
						
						exchange.getIn().setHeader("Object", ioevents.get(i1).getObject());
						exchange.getIn().setHeader("Timestamp", ioevents.get(i1).getTimestamp());
						
						//add to cache
						exchange.getIn().setHeader(CacheConstants.CACHE_OPERATION, CacheConstants.CACHE_OPERATION_CHECK);
						exchange.getIn().setHeader(CacheConstants.CACHE_KEY, key);
						
						exchange.getIn().setHeader("CamelCacheOperation1", "CamelCacheCheck");
						exchange.getIn().setHeader("CamelCacheKey1", key);
						
						logger.debug(String.format("*** CACHE HEADERS: %s %s  ***", CacheConstants.CACHE_OPERATION, CacheConstants.CACHE_OPERATION_CHECK ));
						logger.debug(String.format("*** CACHE HEADERS: %s %s  ***", CacheConstants.CACHE_KEY, key ));
						
						//exchange.getIn().setHeader("TEST", key);
						//exchange.getIn().setHeader("DeviceType", vmevents.get(i).getDeviceType());
						
						
	
						try {
							getProcessor().process(exchange);
							events++;
							
							//File cachefile = new File("sendedEvents.dat");
							//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
							logger.error( String.format("Error while process Exchange message: %s ", e));
						} 
						
											
					}
				}
			}
						
			logger.info( String.format("***Received %d IOs statuses from SQL*** ", statuses));

		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			dataSource.close();
			return 0;
		}
		catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			dataSource.close();
			return 0;
		}
		catch (Error e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			dataSource.close();
			return 0;
		}
		catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		finally
		{
			//return 0;
			dataSource.close();
		}
		
		logger.info( String.format("***Sended to Exchange messages: %d ***", events));
		
		dataSource.close();
		
		//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
	
        return 1;
	}
	
	private static List<Event> genEvents( String dititle, HashMap<String, Object> IOsStatuses ) {
		
		String datetime = IOsStatuses.get("datetime").toString();
		String status_colour = IOsStatuses.get("di_status_colour").toString();
		String status_value = IOsStatuses.get("di_status_value").toString();
		String iotype = IOsStatuses.get("iotype").toString();
				//String ping_colour1 = listVmStatuses.get(0).get("ping_colour").toString();
		//vmuuid  = listVmStatuses.get(0).get("uuid").toString();
		logger.debug(datetime + ": " + datetime );
		logger.debug(status_colour + ": " + status_colour);
		logger.debug(status_value + ": " + status_value);
		logger.debug(iotype + ": " + iotype);

		Event event;
		List<Event> eventList = new ArrayList<Event>();
		// Create Event object for further use
		event = genDeviceObj(dititle, "status", 
				status_colour, status_value, IOsStatuses.get("datetime").toString());
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
		event.setEventsource("OPSM");
		event.setService("OPSM");
		

		//System.out.println(event.toString());
		
		logger.debug(event.toString());
		
		return event;
				
	}
	
	
	
	private static String setRightMessage(String dititle, String object, String status) {
		// TODO Auto-generated method stub
		
		String newmessage = String.format("IO: %s. Статус контакта: %s %s", dititle, object, status);
		return newmessage;
	}

	private List<HashMap<String, Object>> getIOsStatuses(String iotitle, String iotable, String iotype, int ioid, DataSource dataSource) throws SQLException {
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
        	
        	String io_status_col;
        	
        	if ( iotype == "DI" )
        		io_status_col = di_status_col;
        	else 
        		io_status_col = do_status_col;
        	
            pstmt = con.prepareStatement(String.format("SELECT datetime, "
            			+ " %s_colour as di_status_colour, %s_value as di_status_value, '%s' as iotype,  '%s' as iotitle"
            			+ " FROM `%s`"
            			+ " WHERE id = ?"
            			, io_status_col, io_status_col, iotype, iotitle, iotable ));
            pstmt.setInt(1, ioid);
            
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

	private List<HashMap<String, Object>> getIOsAndTable(DataSource dataSource, String iotype) throws SQLException {
		// TODO Auto-generated method stub
        
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
        Connection con = null; 
        PreparedStatement pstmt;
        ResultSet resultset = null;
        try {
        	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
        	
        	String table = "tree_" + iotype; 
        	String sql = String.format("SELECT title, type, tablename, numb as ioid " +
                    "FROM %s " +
                    "WHERE `show` = ?"
                    + " AND tablename <> ?", table);
			
            pstmt = con.prepareStatement(sql);
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
			throw e;
			//return null;

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

	private BasicDataSource setupDataSource() {
		
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