/*
  Copyright 2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
  clean install -Dcontact.address=[hjose@img.com.br]
*/

package com.esri.geoevent.processor.httpHandler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.json.XML;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.Field;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.http.GeoEventHttpClient;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

public class HttpHandler extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable
{
  private static final BundleLogger LOGGER                                = BundleLoggerFactory.getLogger(HttpHandler.class);

  public static final String        MODE_PROPERTY                         = "mode";
  public static final String        CLIENT_URL_PROPERTY                   = "clientURL";
  public static final String        CLIENT_URL_USE_PROXY_PROPERTY         = "useClientURLProxy";
  public static final String        CLIENT_URL_PROXY_PROPERTY             = "clientURLProxy";
  public static final String        CLIENT_PARAMETERS_PROPERTY            = "clientParameters";
  public static final String        HTTP_METHOD_PROPERTY                  = "httpMethod";
  public static final String        ACCEPTABLE_MIME_TYPES_CLIENT_PROPERTY = "acceptableMimeTypesClientMode";
  public static final String        ACCEPTABLE_MIME_TYPES_SERVER_PROPERTY = "acceptableMimeTypesServerMode";
  public static final String        FREQUENCY_PROPERTY                    = "frequency";
  public static final String        POST_BODY_PROPERTY                    = "clientPostBody";
  public static final String        POST_CONTENT_TYPE_PROPERTY            = "postContentType";
  public static final String        HONOR_LAST_MODIFIED_PROPERTY          = "honorLastModified";
  public static final String        USERNAME_PROPERTY                     = "username";
  public static final String        PASSWORD_PROPERTY                     = "password";
  public static final String        USE_LONG_POLLING_PROPERTY             = "useLongPolling";
  public static final String        HEADER_PROPERTY                       = "headers";
  public static final String        POST_FROM_PROPERTY                    = "clientPostFrom";
  public static final String        POST_PARAM_PROPERTY                   = "clientPostParameters";
  public static final String        HTTP_TIMEOUT_VALUE                    = "httpTimeoutValue";
  public static final String        HTTP_APPEND_TO_MESSAGE                = "httpAppendToEnd";
  public static final String        CUSTOM_DATE_FORMAT_PROPERTY_NAME      = "CustomDateFormat";

  private String                    serviceURL;
  protected String                  clientUrl;
  private String                    clientParameters                      = "";
  protected String                  httpMethod;
  private String                    acceptableMimeTypes_server;
  protected String                  acceptableMimeTypes_client;
  private int                       frequency;
  protected String                  postBodyType;
  protected String                  postBody;
  private boolean                   honorLastModified;
  private String                    trackIdField;
  private String                    fieldSeparator;

  private Boolean                   useLongPolling                        = false;
  private String                    headerParams;
  private String                    postFrom;
  private String                    postParams;
  private int                       httpTimeoutValue;
  private String                    eom                                   = "";
  private String                    responseFormat                        = "json";

  private Messaging                 messaging;
  private GeoEventCreator           geoEventCreator;
  private GeoEventProducer          geoEventProducer;
  private HttpHandlerAdapter        httpHandlerAdapter;
  private EventDestination          destination;

  private GeoEventDefinitionManager geoEventDefinitionManager;
  private Map<String, String>       edMapper                              = new ConcurrentHashMap<String, String>();
  private String                    newGeoEventDefinitionName;
  private Date                      lastPollingDateTime;
  private int                       historicalTimespanSeconds;
  private Boolean                   useEpochMilliseconds;

  private HttpHandlerDefinition     processDefinition;

  final Object                      lock1                                 = new Object();
  private static final ObjectMapper mapper                                = new ObjectMapper();

  private String[]                  urlParts;
  private String[]                  headers;
  private String[]                  postBodyParts;
  
  private String                    lastGeoEventDefinitionsGUID;

  ExecutorService                   executor                              = Executors.newFixedThreadPool(20);

  protected HttpHandler(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
    this.processDefinition = (HttpHandlerDefinition) definition;
  }

  public void afterPropertiesSet()
  {
    if (hasProperty("TrackIdField"))
      trackIdField = getProperty("TrackIdField").getValueAsString();

    if (hasProperty("responseFormat"))
      responseFormat = getProperty("responseFormat").getValueAsString();

    if (hasProperty("fieldSeparator"))
      fieldSeparator = getProperty("fieldSeparator").getValueAsString();

    if (hasProperty(CLIENT_URL_PROPERTY))
      serviceURL = getProperty(CLIENT_URL_PROPERTY).getValueAsString();

    if (hasProperty(HTTP_METHOD_PROPERTY))
      httpMethod = getProperty(HTTP_METHOD_PROPERTY).getValueAsString();

    String stringValue = "";
    try
    {
      if (hasProperty(FREQUENCY_PROPERTY))
      {
        stringValue = getProperty(FREQUENCY_PROPERTY).getValueAsString();
        frequency = Integer.parseInt(stringValue);
      }
    }
    catch (NumberFormatException ex)
    {
      LOGGER.error("INT_PARSE_ERROR", FREQUENCY_PROPERTY, stringValue);
    }
    if (hasProperty(POST_BODY_PROPERTY))
      postBody = getProperty(POST_BODY_PROPERTY).getValueAsString();
    if (hasProperty(HEADER_PROPERTY))
    {
      headerParams = getProperty(HEADER_PROPERTY).getValueAsString();
      if (headerParams.isEmpty() == false)
      {
        headers = headerParams.split("[|]");        
      }
    }

    if (hasProperty(HTTP_TIMEOUT_VALUE))
    {
      String secStr = getProperty(HTTP_TIMEOUT_VALUE).getValueAsString();
      try
      {
        long sec = Long.parseLong(getProperty(HTTP_TIMEOUT_VALUE).getValueAsString());
        if (sec < 0 || sec * 1000L > Integer.MAX_VALUE)
          LOGGER.error("INVALID_TIMEOUT_VALUE_NO_CHANGE", secStr, (int) httpTimeoutValue / 1000);
        else
          httpTimeoutValue = (int) (sec * 1000L);
      }
      catch (NumberFormatException ex)
      {
        LOGGER.error("INT_PARSE_ERROR", HTTP_TIMEOUT_VALUE, secStr);
      }      
    }
        
    if (hasProperty("historicalTimespanSeconds"))
    {
      historicalTimespanSeconds = Integer.parseInt(getProperty("historicalTimespanSeconds").getValueAsString());
      Calendar calendar = Calendar.getInstance(); // gets a calendar using the default time zone and locale.
      calendar.add(Calendar.SECOND, -historicalTimespanSeconds);
      lastPollingDateTime = calendar.getTime();
    }
    
    if (hasProperty("useEpochMilliseconds"))
    {
      useEpochMilliseconds = Boolean.parseBoolean(getProperty("useEpochMilliseconds").getValueAsString());
    }

    if (httpHandlerAdapter == null)
    {
      httpHandlerAdapter = new HttpHandlerAdapter(geoEventCreator, geoEventProducer, processDefinition, getId(), trackIdField);
    }
    httpHandlerAdapter.afterPropertiesSet(this);
  }

  @Override
  public void setId(String id)
  {
    super.setId(id);

    destination = new EventDestination(getId() + ":event");
    geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
  }

  @Override
  public GeoEvent process(GeoEvent geoevent) throws Exception
  {
    GeoEventDefinition gd = geoevent.getGeoEventDefinition();
    // "http://server/{f1}/folder/{f2}?value={f3}";
    urlParts = serviceURL.split("[{*}]");
    String[] tempUrlParts = Arrays.copyOf(urlParts, urlParts.length);
    String newURL = "";
    String newPostBody = "";
    for (int i = 0; i < tempUrlParts.length; i++)
    {
      Integer idx = gd.getIndexOf(tempUrlParts[i]);
      // LOGGER.info(tempUrlParts[i].toString() + ":" + idx.toString());
      if (idx >= 0)
      {
        Field field = geoevent.getField(new FieldExpression(tempUrlParts[i]));
        if (field != null)
        {
          String fieldValue = field.getValue().toString();
          tempUrlParts[i] = fieldValue;
          if (fieldValue != null)
          {
            // LOGGER.info("Got " + field.getDefinition().getName() + " " + tempUrlParts[i]);
          }
        }
      }
      else
      {
         //field not found - add lastpollDateTime and currentDateTime URL params
        if (tempUrlParts[i].equals("$lastPollingDateTime"))
        {
          Long timeValue = (Long)lastPollingDateTime.getTime();
          if (useEpochMilliseconds == false)
          {
            timeValue = timeValue / 1000;
          }            
          tempUrlParts[i] = timeValue.toString();
        }
        else if(tempUrlParts[i].equals("$currentDateTime"))
        {
          Date currentDateTime = new Date();
          Long timeValue = (Long)currentDateTime.getTime();
          if (useEpochMilliseconds == false)
          {
            timeValue = timeValue / 1000;
          }            
          tempUrlParts[i] = timeValue.toString();            
        }
      }
      
      newURL += tempUrlParts[i];
    }

    if (httpMethod.equals("POST"))
    {
      postBodyParts = postBody.split("[{*}]");
      String[] tempPostBodyParts = Arrays.copyOf(postBodyParts, postBodyParts.length);
      for (int i = 0; i < tempPostBodyParts.length; i++)
      {
        Integer idx = gd.getIndexOf(tempPostBodyParts[i]);
        // LOGGER.info(tempPostBodyParts[i].toString() + ":" + idx.toString());
        if (idx >= 0)
        {
          Field field = geoevent.getField(new FieldExpression(tempPostBodyParts[i]));
          if (field != null)
          {
            String fieldValue = field.getValue().toString();
            tempPostBodyParts[i] = fieldValue;
            if (fieldValue != null)
            {
              // LOGGER.info("Got " + field.getDefinition().getName() + " " + tempPostBodyParts[i]);
            }
          }
          else
          {
            // add lastpollDateTime and currentDateTime PostBody
            if (tempPostBodyParts[i].equals("$lastPollingDateTime"))
            {
              Long timeValue = (Long)lastPollingDateTime.getTime();
              if (useEpochMilliseconds == false)
              {
                timeValue = timeValue / 1000;
              }
              tempPostBodyParts[i] = timeValue.toString();
            }
            else if(tempPostBodyParts[i].equals("$currentDateTime"))
            {
              Date currentDateTime = new Date();
              Long timeValue = (Long)currentDateTime.getTime();
              if (useEpochMilliseconds == false)
              {
                timeValue = timeValue / 1000;
              }
              tempPostBodyParts[i] = timeValue.toString();            
            }
          }
        }
        newPostBody += tempPostBodyParts[i]; 
      }
    }
    LOGGER.debug("New URL " + newURL);
    if(httpMethod.equals("POST"))
    {
      LOGGER.debug("New PostBody " + newPostBody);    
    }
    String[] processedHeaders = new String[headers.length];
    List<FieldDefinition> fdl  =  gd.getFieldDefinitions();
    for (int i = 0; i < headers.length; i++)
    {
      String[] nameValue = headers[i].split(":");
      // Process header name
      String[] nameParts = nameValue[0].split("[{*}]");
      String processedName = "";
      for (String part : nameParts)
      {
        
        if (part != null && !part.isEmpty()) {
	        	Integer idx = gd.getIndexOf(part);
		        if (idx >= 0)
		        {
		          Field field = geoevent.getField(new FieldExpression(part));
		          if (field != null)
		          {
		            part = field.getValue().toString();
		          }
		        }
        }
        processedName += part;
      }

      // Process header value
      String processedValue = "";
      if (nameValue.length > 1)  // To avoid ArrayIndexOutOfBoundsException
      {
        String[] valueParts = nameValue[1].split("[{*}]");
        for (String part : valueParts)
        {
        	if (part != null && !part.isEmpty()) {	
			      Integer idx = gd.getIndexOf(part);
			      if (idx >= 0)
			      {
			        Field field = geoevent.getField(new FieldExpression(part));
			        if (field != null)
			        {
			          part = field.getValue().toString();
			        }
			      }
        	}
          processedValue += part;
        }
      } 
      else 
      {
        LOGGER.error("Invalid header format for: " + headers[i]);
      }

      // Store the processed header
      processedHeaders[i] = processedName + ":" + processedValue;
    }

    // Set processed headers for use in getFeed
    headers = processedHeaders;
    LOGGER.info("headers: \n" + headers.toString());
    HttpRequester httpRequester = new HttpRequester(newURL, newPostBody);
    executor.execute(httpRequester);

    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    List<String> errors = new ArrayList<String>();

    if (errors.size() > 0)
    {
      StringBuffer sb = new StringBuffer();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(LOGGER.translate("VALIDATION_ERROR", this.getClass().getName(), sb.toString()));
    }
  }

  @Override
  public void onServiceStart()
  {
  }

  @Override
  public void onServiceStop()
  {
  }

  @Override
  public void shutdown()
  {
    super.shutdown();
    if (executor != null)
    {
      executor.shutdown();
      while (!executor.isTerminated())
      {
      }
      executor = null;
    }

    clearGeoEventDefinitionMapper();
  }

  @Override
  public EventDestination getEventDestination()
  {
    return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException
  {
    if (geoEventProducer != null && geoEvent != null)
      geoEventProducer.send(geoEvent);
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
    geoEventCreator = messaging.createGeoEventCreator();
  }

  public void setGeoEventDefinitionManager(GeoEventDefinitionManager geoEventDefinitionManager)
  {
    this.geoEventDefinitionManager = geoEventDefinitionManager;
  }

  @Override
  public void disconnect()
  {
    if (geoEventProducer != null)
      geoEventProducer.disconnect();
  }

  @Override
  public String getStatusDetails()
  {
    return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
  }

  @Override
  public void init() throws MessagingException
  {
    afterPropertiesSet();
  }

  @Override
  public boolean isConnected()
  {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException
  {
    ;
  }

  @Override
  public void update(Observable o, Object arg)
  {
    ;
  }

  synchronized private void clearGeoEventDefinitionMapper()
  {
    if (!edMapper.isEmpty())
    {
      for (String guid : edMapper.values())
      {
        try
        {
          geoEventDefinitionManager.deleteGeoEventDefinition(guid);
        }
        catch (GeoEventDefinitionManagerException e)
        {
          ;
        }
      }
      edMapper.clear();
    }
  }

  private String xmlToJson(String responseBody)
  {
    String json = "";
    //if (responseBody.substring(0, 10).contains("<?xml"))
    {
      JSONObject jobj = XML.toJSONObject(responseBody);
      json = jobj.toString();
      LOGGER.debug(json);
    }

    return json;
  }

  private String csvToJson(String responseBody)
  {
    //TODO - test this
    String geoEventDefinitionName = httpHandlerAdapter.getGeoEventDefinitionName();
    String[] values = responseBody.split(fieldSeparator);
    String json = "{\"" + geoEventDefinitionName + "\" : ";
    if (httpHandlerAdapter.getCreateGeoEventDefinition())
    {
      for (Integer i = 0; i < values.length; i++)
      {
        if (NumberUtils.isCreatable(values[i]))
        {
          json += "\"field" + i.toString() + "\":" + values[i];
        }
        else
        {
          json += "\"field" + i.toString() + "\":\"" + values[i] + "\"";
        }
        if (i < values.length - 1)
        {
          json += ",";
        }
      }
    }
    else
    {
      int perfectSize = values.length;
      if (httpHandlerAdapter.getBuildGeometryFromFields())
        perfectSize++;

      GeoEventDefinition geoEventDefinition = null;
      if (lastGeoEventDefinitionsGUID != null)
      {
        GeoEventDefinition def = geoEventCreator.getGeoEventDefinitionManager().getGeoEventDefinition(lastGeoEventDefinitionsGUID);
        // if the old definition still exists and hasn't been modified structurally, just reuse it.
        if (def != null && def.getFieldDefinitions().size() == perfectSize)
          geoEventDefinition = def;
      }

      if (geoEventDefinition == null)
      {
        Collection<GeoEventDefinition> searchResults = geoEventCreator.getGeoEventDefinitionManager().searchGeoEventDefinitionByName(geoEventDefinitionName);
        for (GeoEventDefinition candidate : searchResults)
        {
          if (candidate.getFieldDefinitions().size() == perfectSize)
          {
            geoEventDefinition = candidate;
            break;
          }
        }
        if (geoEventDefinition == null)
          geoEventDefinition = searchResults.iterator().next();
      }
      if (geoEventDefinition == null)
      {
        LOGGER.error("GED_DOESNT_EXIST");
        return null;
      }
      lastGeoEventDefinitionsGUID = geoEventDefinition.getGuid();

      List<FieldDefinition> fds = geoEventDefinition.getFieldDefinitions();
      for (Integer i = 0; i < values.length; i++)
      {
        FieldDefinition fd = fds.get(i);
        String fieldName = fd.getName();
        if (NumberUtils.isCreatable(values[i]))
        {
          json += "\"" + fieldName + "\":" + values[i];
        }
        else
        {
          json += "\"" + fieldName + "\":\"" + values[i] + "\"";
        }
        if (i < values.length - 1)
        {
          json += ",";
        }
      }
    }
    json += "}";

    LOGGER.debug(json);

    return json;
  }

  private void getFeed(String endpointURL, String postPayload)
  {
    // System.out.println("getFeed: " + messageType);
    GeoEventHttpClient geHttp = HttpHandlerService.httpClientService.createNewClient();

    try
    {
      URL url = new URL(endpointURL);
      String queryString = "";
      HttpRequestBase httpRequest = null;
      if(httpMethod.equals("GET"))
      {
        httpRequest = geHttp.createGetRequest(url, queryString);
      }
      else if(httpMethod.equals("POST"))
      {
        httpRequest = geHttp.createPostRequest(url, postPayload, postBodyType);
      }
      else if(httpMethod.equals("PUT"))
      {
        geHttp.createPutRequest(url, postPayload.getBytes(), postBodyType);
      }

      if (headers != null && headers.length > 0) 
      {
        for (int i = 0; i < headers.length; i++)
        {
          String[] nameValue = headers[i].split(":");
          httpRequest.addHeader(nameValue[0], nameValue[1]);                   
        }
      }

      try
      {
        HttpResponse response = null;
        response = geHttp.execute(httpRequest, GeoEventHttpClient.DEFAULT_TIMEOUT);   
        
        HttpEntity entity = (response != null) ? response.getEntity() : null;

        if (entity != null)
        {
          LOGGER.debug("Got response from http request.");
        }

        StatusLine statusLine = response.getStatusLine();

        if (statusLine.getStatusCode() != HttpStatus.SC_OK)
        {
          String message = httpRequest.getRequestLine().getUri() + " :  Request failed(" + statusLine.toString() + ")";
          LOGGER.error(message);
        }

        try
        {
          String responseBody = EntityUtils.toString(entity);
          LOGGER.debug(responseBody);
          System.out.println(responseBody);

          if (responseFormat.equals("xml"))
          {
            responseBody = xmlToJson(responseBody);
          }
          else if (responseFormat.equalsIgnoreCase("csv"))
          {
            responseBody = csvToJson(responseBody);
          }

          // Send Message
          try
          {
            if (responseBody != null)
            {
              httpHandlerAdapter.receive(responseBody);
            }
          }
          catch (Exception e)
          {
            LOGGER.error(e.getMessage());
          }
        }
        catch (ParseException | IOException e)
        {
          LOGGER.error("getFeed " + e.getMessage());
        }
      }
      catch (IOException e1)
      {
        LOGGER.error("getFeed " + e1.getMessage());
      }
    }
    catch (MalformedURLException e1)
    {
      LOGGER.error("getFeed " + e1.getMessage());
    }
  }

  class HttpRequester implements Runnable
  {
    private String endpointURL;
    private String postPayload;

    public HttpRequester(String endpointURL, String postPayload)
    {
      this.endpointURL = endpointURL;
      this.postPayload = postPayload;
    }

    @Override
    public void run()
    {
      getFeed(endpointURL, postPayload);
    }
  }
}
