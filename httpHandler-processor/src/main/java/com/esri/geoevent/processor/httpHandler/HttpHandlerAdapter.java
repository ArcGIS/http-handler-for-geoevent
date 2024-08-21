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
*/

package com.esri.geoevent.processor.httpHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.adapter.StringBuilderCacheItem;
import com.esri.ges.core.Uri;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.util.StringUtil;

public class HttpHandlerAdapter implements Runnable
{
  private static final BundleLogger           LOGGER                                          = BundleLoggerFactory.getLogger(HttpHandlerAdapter.class);

  private ObjectMapper                        mapper                                          = new ObjectMapper();
  private boolean                             creatingGeoEventDefinition                      = false;                                                  // Always
                                                                                                                                                        // false
                                                                                                                                                        // for
                                                                                                                                                        // this
                                                                                                                                                        // case

  private GeoEventCreator                     geoEventCreator;
  private GeoEventProducer                    geoEventProducer;

  private String                              geoEventDefinitionName;
  protected String                            lastGeoEventDefinitionsGUID                     = null;

  private String                              jsonObjectName;

  private boolean                             buildGeometryFromFields                         = true;
  private String                              xGeometryField                                  = "lon";
  private String                              yGeometryField                                  = "lat";
  private String                              zGeometryField                                  = "alt";
  private String                              wkidGeometryField;
  private String                              wkTextGeometryField;
  private boolean                             isLearningMode                                  = false;
  private String                              id;
  private String                              trackIdField;
  private String                              customDateFormat;
  private SimpleDateFormat                    customDateParser                                = null;

  private HttpHandlerDefinition               definition;
  
  // Maximum Buffer Size is 100 MB
  private int                                 maxStringBuilderSize                            = 100 * 1024 * 1024;
  private volatile boolean                    isRunning                                       = false;
  private int                                 cleanInterval                                   = 1;                                                      // in
                                                                                                                                                        // minutes
  private Thread                              cleaningThread;
  private Map<String, StringBuilderCacheItem> stringBuilderCache;

  public static final String                  JSON_OBJECT_NAME                                = "JsonObjectName";
  public static final String                  EXISTING_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME = "ExistingGeoEventDefinitionName";
  public static final String                  NEW_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME      = "NewGeoEventDefinitionName";
  public static final String                  CREATE_GEOEVENT_DEFINITION_PROPERTY_NAME        = "CreateGeoEventDefinition";
  public static final String                  CUSTOM_DATE_FORMAT_PROPERTY_NAME                = "CustomDateFormat";
  public static final String                  BUILD_GEOMETRY_FROM_FIELDS_PROPERTY_NAME        = "BuildGeometryFromFields";
  public static final String                  X_GEOMETRY_FIELD_PROPERTY_NAME                  = "XGeometryField";
  public static final String                  Y_GEOMETRY_FIELD_PROPERTY_NAME                  = "YGeometryField";
  public static final String                  Z_GEOMETRY_FIELD_PROPERTY_NAME                  = "ZGeometryField";
  public static final String                  WKID_GEOMETRY_FIELD_PROPERTY_NAME               = "WKIDGeometryField";
  public static final String                  WK_TEXT_GEOMETRY_FIELD_PROPERTY_NAME            = "WKTextGeometryField";
  public static final String                  JSON_IS_LEARNING_MODE_PROPERTY_NAME             = "isLearningMode";

  public HttpHandlerAdapter(GeoEventCreator geoEventCreator, GeoEventProducer geoEventProducer, HttpHandlerDefinition definition, String id, String trackIdField)
  {
    LOGGER.debug("EventGenerator created");
    this.geoEventCreator = geoEventCreator;
    this.geoEventProducer = geoEventProducer;
    this.definition = definition;
    this.id = id;
    this.trackIdField = trackIdField;
    
    stringBuilderCache = new ConcurrentHashMap<String, StringBuilderCacheItem>();

    // start the cleaning thread
    if (cleaningThread == null)
    {
      isRunning = true;
      cleaningThread = new Thread(this);
      cleaningThread.setName("JsonInboundAdapterCleaningThread" + hashCode());
      cleaningThread.setDaemon(true);
      cleaningThread.start();
    }
  }
  
  public String getGeoEventDefinitionName()
  {
	return geoEventDefinitionName;
  }
  
  public Boolean getCreateGeoEventDefinition()
  {
	return creatingGeoEventDefinition;
  }
  
  public Boolean getBuildGeometryFromFields()
  {
	return buildGeometryFromFields;  
  }
  
  public void afterPropertiesSet(HttpHandler httpHandler)
  {
    customDateParser = null;
    jsonObjectName = null;
    lastGeoEventDefinitionsGUID = null;
    if (httpHandler.hasProperty(JSON_OBJECT_NAME))
    {
      jsonObjectName = httpHandler.getProperty(JSON_OBJECT_NAME).getValueAsString();
      if (jsonObjectName != null && jsonObjectName.trim().equals(""))
        jsonObjectName = null;
    }
    if (httpHandler.hasProperty(CREATE_GEOEVENT_DEFINITION_PROPERTY_NAME))
      creatingGeoEventDefinition = ((Boolean) (httpHandler.getProperty(CREATE_GEOEVENT_DEFINITION_PROPERTY_NAME).getValue())).booleanValue();
    if (httpHandler.hasProperty(EXISTING_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME))
      geoEventDefinitionName = httpHandler.getProperty(EXISTING_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME).getValueAsString();
    if (creatingGeoEventDefinition && httpHandler.hasProperty(NEW_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME))
      geoEventDefinitionName = httpHandler.getProperty(NEW_GEOEVENT_DEFINITION_NAME_PROPERTY_NAME).getValueAsString();
    if (httpHandler.hasProperty(CUSTOM_DATE_FORMAT_PROPERTY_NAME))
    {
      customDateFormat = httpHandler.getProperty(CUSTOM_DATE_FORMAT_PROPERTY_NAME).getValueAsString();
      if (customDateFormat != null && !customDateFormat.trim().isEmpty())
        customDateParser = new SimpleDateFormat(customDateFormat);
    }
    
    if (httpHandler.hasProperty(BUILD_GEOMETRY_FROM_FIELDS_PROPERTY_NAME))
    {
      buildGeometryFromFields = ((Boolean) (httpHandler.getProperty(BUILD_GEOMETRY_FROM_FIELDS_PROPERTY_NAME).getValue())).booleanValue();
      if (buildGeometryFromFields)
      {
        if (httpHandler.hasProperty(X_GEOMETRY_FIELD_PROPERTY_NAME))
        {
          xGeometryField = httpHandler.getProperty(X_GEOMETRY_FIELD_PROPERTY_NAME).getValueAsString();
        }
        if (httpHandler.hasProperty(Y_GEOMETRY_FIELD_PROPERTY_NAME))
        {
          yGeometryField = httpHandler.getProperty(Y_GEOMETRY_FIELD_PROPERTY_NAME).getValueAsString();
        }
        if (httpHandler.hasProperty(Z_GEOMETRY_FIELD_PROPERTY_NAME))
        {
          zGeometryField = httpHandler.getProperty(Z_GEOMETRY_FIELD_PROPERTY_NAME).getValueAsString();
        }
        if (httpHandler.hasProperty(WKID_GEOMETRY_FIELD_PROPERTY_NAME))
        {
          wkidGeometryField = httpHandler.getProperty(WKID_GEOMETRY_FIELD_PROPERTY_NAME).getValueAsString();
        }
        if (httpHandler.hasProperty(WK_TEXT_GEOMETRY_FIELD_PROPERTY_NAME))
        {
          wkTextGeometryField = httpHandler.getProperty(WK_TEXT_GEOMETRY_FIELD_PROPERTY_NAME).getValueAsString();
        }
      }
    }
    else
      buildGeometryFromFields = false;
    
    if (httpHandler.hasProperty(JSON_IS_LEARNING_MODE_PROPERTY_NAME))
      isLearningMode = ((Boolean) (httpHandler.getProperty(JSON_IS_LEARNING_MODE_PROPERTY_NAME).getValue())).booleanValue();
  }


  public void receive(String json)
  {
    // LOGGER.debug("ChannelId: " + channelId);
    // check the cache for an existing string buffer - we might not be done with it
    StringBuilder stringBuilder = new StringBuilder(json);

    String remainingString = "";
    try
    {
      String jsonStrings = StringUtil.removeUTF8BOM(stringBuilder.toString());

      List<String> objectStrings = parseToIndividualObjects(jsonStrings);
      for (String jsonString : objectStrings)
      {
        remainingString = jsonString;
        JsonNode tree = mapper.readTree(jsonString);
        // findNodes(tree, jsonObjectName);
        JsonInboundParser parser = getJSONParser(geoEventDefinitionName);
        parser.findNodes(tree, jsonObjectName, geoEventProducer);
      }
    }
    catch (IOException ex)
    {
      String thisError = ex.getMessage();
      String partialJSONError = "Unexpected end-of-input";
      if (thisError.startsWith(partialJSONError))
      {
        ///
      }
      Integer count = stringBuilderCache.size();
      LOGGER.info("StringBuilderCache Count: " + count.toString());
      LOGGER.error("PARSE_ERROR");
      LOGGER.info(ex.getMessage(), ex);
    }
    catch (Exception ex)
    {
      LOGGER.debug(ex.getMessage(), ex);
    }
  }

  private List<String> parseToIndividualObjects(String inputString) throws JsonProcessingException, IOException
  {
      List<String> results = new ArrayList<>();
      JsonFactory factory = new JsonFactory();
      ObjectMapper mapper = new ObjectMapper(factory);

      try (JsonParser parser = factory.createParser(inputString)) {
          int depth = 0;
          int start = 0;
          JsonToken currentToken;

          while ((currentToken = parser.nextToken()) != null) {
              if (currentToken.isStructStart()) {
                  if (depth == 0) {
                      start = (int) parser.getTokenLocation().getCharOffset();
                  }
                  depth++;
              } else if (currentToken.isStructEnd()) {
                  depth--;
                  if (depth == 0) {
                      int end = (int) parser.getTokenLocation().getCharOffset() + 1;
                      String jsonObjectString = inputString.substring(start, end);
                      results.add(jsonObjectString);
                      start = end;
                  }
              }
          }

          // If there is leftover JSON after parsing, add it to the results
          if (depth > 0) {
              String leftovers = inputString.substring(start);
              results.add(leftovers);
          }
      } catch (JsonProcessingException e) {
          throw new IOException("Error processing JSON", e);
      }

      return results;

  }

  public static void main(String[] args) throws JsonParseException, IOException
  {
    String test = "{\"name\":\"ryan\"}{\"name\":\"ric";
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);
    JsonParser outerParser = factory.createParser(test);
    
    Iterator<JsonNode> itr = mapper.readValues(outerParser, JsonNode.class);
    while (itr.hasNext())
    {
      try
      {
        JsonNode tree = itr.next();
        System.out.println("tree : " + tree);
      }
      catch (RuntimeException ex)
      {
        Throwable cause = ex.getCause();
        if (cause instanceof JsonParseException)
        {
          JsonParseException jpe = (JsonParseException) cause;
          System.out.println(jpe);
          jpe.getLocation();
          JsonLocation location = jpe.getLocation();
          System.out.println("Location : " + location);
          int start = (int) jpe.getLocation().getCharOffset();
          System.out.println("Start is " + start);
          String leftovers = test.substring(start);
          System.out.println("leftovers : " + leftovers);
          ex.printStackTrace();

        }
      }
    }
  }

  private JsonInboundParser getJSONParser(String geoEventDefName)
  {
    Uri uri = new Uri("auto-generated", definition.getDomain() + "." + definition.getName(), definition.getVersion());
    
    JsonInboundParser parser = new JsonInboundParser(creatingGeoEventDefinition, geoEventDefinitionName, 
        buildGeometryFromFields, xGeometryField, yGeometryField, zGeometryField, wkidGeometryField, 
        wkTextGeometryField, customDateFormat, geoEventCreator, uri, id, trackIdField);

    return parser;
  }

  /**
   * Added the run() method to cleanup its stringBuilder cache
   */
  @Override
  public void run()
  {
    while (isRunning)
    {
      try
      {
        synchronized (stringBuilderCache)
        {
          if (!stringBuilderCache.isEmpty())
          {
            StringBuilderCacheItem cacheItem;
            for (String channelId : stringBuilderCache.keySet())
            {
              cacheItem = stringBuilderCache.get(channelId);
              if (cacheItem.isExpired())
                stringBuilderCache.remove(channelId);
            }
          }
        }
        try
        {
          Thread.sleep(cleanInterval * 60 * 1000);
        }
        catch (InterruptedException ex)
        {
          ;
        }
      }
      catch (Exception ex)
      {
        LOGGER.debug(ex.getMessage(), ex);
      }
    }
  }

  public static void consoleDebugPrintLn(String msg)
  {
    String consoleOut = System.getenv("GEP_CONSOLE_OUTPUT");
    if (consoleOut != null && "1".equals(consoleOut))
    {
      System.out.println(msg);
    }
  }

  public static void consoleDebugPrint(String msg)
  {
    String consoleOut = System.getenv("GEP_CONSOLE_OUTPUT");
    if (consoleOut != null && "1".equals(consoleOut))
    {
      System.out.print(msg);
    }
  }
}
