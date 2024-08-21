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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

import com.esri.core.geometry.Line;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.ges.core.AccessType;
import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.Uri;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldCardinality;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldGroup;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.util.Converter;
import com.esri.ges.util.DateUtil;
import com.esri.ges.util.GeometryUtil;
import com.esri.ges.util.Validator;

public class JsonInboundParser
{
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(JsonInboundParser.class);

  // properties
  private boolean                   creatingGeoEventDefinition;
  private String                    geoEventDefinitionName;
  private boolean                   buildGeometryFromFields;
  private String                    xGeometryField;
  private String                    yGeometryField;
  private String                    zGeometryField;
  private String                    wkidGeometryField;
  private String                    wkTextGeometryField;
  private String                    customDateFormat;
  private String                    trackIdField;

  // utilities
  private GeoEventCreator           geoEventCreator;
  private Uri                       uri;
  private String                    id;

  // local
  private String                    lastGeoEventDefinitionsGUID;
  private boolean                   haveDate;
  private boolean                   haveGeometry;

  public JsonInboundParser(boolean creatingGeoEventDefinition, String
                            geoEventDefinitionName, boolean buildGeometryFromFields, String
                            xGeometryField, String yGeometryField, String zGeometryField, String
                            wkidGeometryField, String wkTextGeometryField, String customDateFormat,
                            GeoEventCreator geoEventCreator, Uri uri, String id, String trackIdField)
  {
    this.creatingGeoEventDefinition = creatingGeoEventDefinition;
    this.geoEventDefinitionName = geoEventDefinitionName;
    this.buildGeometryFromFields = buildGeometryFromFields;
    this.xGeometryField = xGeometryField;
    this.yGeometryField = yGeometryField;
    this.zGeometryField = zGeometryField;
    this.wkidGeometryField = wkidGeometryField;
    this.wkTextGeometryField = wkTextGeometryField;
    this.customDateFormat = customDateFormat;
    this.geoEventCreator = geoEventCreator;
    this.uri = uri;
    this.id = id;
    this.trackIdField = trackIdField;
  }

  public void findNodes(JsonNode tree, String nodeName, GeoEventProducer geoEventProducer)
  {
    if (tree.isArray())
    {
      Iterator<JsonNode> elements = tree.elements();
      while (elements.hasNext())
      {
        JsonNode element = elements.next();
        findNodes(element, nodeName, geoEventProducer);
      }
    }
    if (tree.isObject())
    {
      if (nodeName == null)
      {
        GeoEvent event = makeGeoEvent(tree);
        if (geoEventProducer != null && event != null)
        {
          try
          {
            geoEventProducer.send(event);
          }
          catch (MessagingException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
      else if (tree.has(nodeName))
      {
        findNodes(tree.get(nodeName), null, geoEventProducer);
      }
      else
      {
        Iterator<JsonNode> elements = tree.elements();
        while (elements.hasNext())
        {
          JsonNode element = elements.next();
          findNodes(element, nodeName, geoEventProducer);
        }
      }
    }
  }

  private GeoEvent makeGeoEvent(JsonNode node)
  {
    int perfectSize = node.size();
    if (buildGeometryFromFields)
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
      if (searchResults.size() == 0)
      {
        if (creatingGeoEventDefinition)
        {
          try
          {
            geoEventDefinition = deriveGeoEventDefinition(node);
            if (geoEventDefinition != null)
              geoEventCreator.getGeoEventDefinitionManager().addGeoEventDefinition(geoEventDefinition);
          }
          catch (ConfigurationException e)
          {
            LOGGER.error("GED_CREATION_ERROR", e.getMessage(), node.toString());
          }
          catch (GeoEventDefinitionManagerException e)
          {
            LOGGER.error("GED_CREATION_ERROR", e.getMessage(), node.toString());
          }
        }
      }
      else
      {
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
    }
    if (geoEventDefinition == null)
    {
      LOGGER.error("GED_DOESNT_EXIST");
      return null;
    }
    
    lastGeoEventDefinitionsGUID = geoEventDefinition.getGuid();
    GeoEvent event = null;
    try
    {
      event = geoEventCreator.create(geoEventDefinition.getGuid());

      event.setProperty(GeoEventPropertyName.TYPE, "event");
      event.setProperty(GeoEventPropertyName.OWNER_ID, id);
      event.setProperty(GeoEventPropertyName.OWNER_URI, uri);

      populateGeoEvent(event, node, geoEventDefinition.getFieldDefinitions());
      constructGeometry(event);
    }
    catch (MessagingException e)
    {
      LOGGER.error("GE_CREATION_ERROR", e, e.getMessage());
    }
    catch (FieldException e)
    {
      LOGGER.error("GE_GEOMETRY_CREATION_ERROR", e);
    }
    return event;
  }

  private void constructGeometry(GeoEvent event) throws FieldException
  {
    if (buildGeometryFromFields)
    {
      if (xGeometryField != null && yGeometryField != null)
      {
        double x = 0D;
        double y = 0D;
        double z = 0D;
        int wkid = 4326;
        Object xObject = event.getField(xGeometryField);
        if (xObject != null)
          x = Converter.convertToDouble(xObject);
        else
          LOGGER.warn("X Geometry field is null", xGeometryField, "x");
        Object yObject = event.getField(yGeometryField);
        if (yObject != null)
          y = Converter.convertToDouble(yObject);
        else
          LOGGER.warn("Y Geometry field is null", yGeometryField, "y");
        if (zGeometryField != null)
        {
          Object zObject = event.getField(zGeometryField);
          if (zObject != null)
            z = Converter.convertToDouble(zObject);
          else
            LOGGER.debug("Z Geometry field is null", zGeometryField, "z");        	  
        }
        if (!Validator.isEmpty(wkidGeometryField))
        {
          Object wkidObject = event.getField(wkidGeometryField);
          if (wkidObject != null)
            wkid = Converter.convertToInteger(wkidObject);
        }
        else
        {
          if (!Validator.isEmpty(wkTextGeometryField))
          {
            String wkText = (String) event.getField(wkTextGeometryField);
            wkid = SpatialReference.create(wkText).getID();
          }
        }
        MapGeometry point = new MapGeometry(new Point(x, y, z), SpatialReference.create(wkid));
        event.setGeometry(point);
      }
    }
  }

  private void ConstructPolylineGeometry(GeoEvent event)
  {
    int wkid = 4326;
    try
    {
      boolean waypoints4d = false;
      List<FieldGroup> fieldgroups = event.getFieldGroups("waypoints");
      if (fieldgroups == null)
      {
        LOGGER.debug("waypoints fieldgroup not found.");
        // Try 4d_waypoints
        fieldgroups = event.getFieldGroups("4d_waypoints");
        if (fieldgroups == null)
        {
          LOGGER.debug("4d_waypoints fieldgroup not found.");
          return;
        }
        else
        {
          waypoints4d = true;
        }
      }

      Polyline polyline = new Polyline();
      int index = 0;
      for (FieldGroup fg : fieldgroups)
      {
        double x = 0D;
        double y = 0D;
        double z = 0D;
        Object xObject = fg.getField("lon");
        if (xObject != null)
        {
          x = Converter.convertToDouble(xObject);
        }
        else
          LOGGER.warn("FIELD_PARSE_ERROR", "lon", "x");
        Object yObject = fg.getField("lat");
        if (yObject != null)
          y = Converter.convertToDouble(yObject);
        else
          LOGGER.warn("FIELD_PARSE_ERROR", "lat", "y");
        if (waypoints4d == true)
        {
          Object zObject = fg.getField("alt");
          if (zObject != null)
            z = Converter.convertToDouble(zObject);
          else
            LOGGER.warn("FIELD_PARSE_ERROR", "alt", "z");
        }
        if (index == 0)
        {
          LOGGER.debug("startPath: " + x + ", " + y);
          polyline.startPath(x, y);
        }
        else
        {
          LOGGER.debug("lineTo: " + x + ", " + y);
          polyline.lineTo(x, y);
        }
        index++;
      }
      Line meridian = new Line();
      meridian.setStart(new Point(0.0, 89.99999));
      meridian.setEnd(new Point(0.0, -89.99999));

      try
      {
        MapGeometry pline = new MapGeometry(polyline, SpatialReference.create(wkid));
        LOGGER.debug(pline.toString());
        event.setGeometry(pline);
      }
      catch (FieldException e)
      {
        LOGGER.error(e.getMessage());
      }
    }
    catch (FieldException e1)
    {
      LOGGER.error(e1.getMessage());
    }
  }

  private void populateGeoEvent(FieldGroup event, JsonNode node, List<FieldDefinition> fds)
  {
    if (node != null && node.isObject())
    {
      for (Iterator<String> fieldNames = node.fieldNames(); fieldNames != null && fieldNames.hasNext();)
      {
        String fieldName = null;
        try
        {
          fieldName = fieldNames.next();
          for (FieldDefinition fd : fds)
          {
            if (fd.getName().equals(fieldName))
            {
              JsonNode n = node.get(fieldName);
              if (n != null)
              {
                switch (fd.getCardinality())
                {
                  case One:
                    event.setField(fieldName, convert(event, fd, n));
                    break;
                  case Many:
                  {
                    if (n.isArray())
                    {
                      ArrayList<Object> results = new ArrayList<Object>();
                      for (Iterator<JsonNode> arrayElements = n.elements(); arrayElements.hasNext();)
                        results.add(convert(event, fd, arrayElements.next()));
                      event.setField(fieldName, results);
                    }
                  }
                    break;
                }
              }
            }
          }
        }
        catch (FieldException ex)
        {
          LOGGER.error("FIELD_ERROR", fieldName, ex.getMessage());
        }
      }
    }
  }

  private Object getJsonNodeValue(JsonNode node)
  {
    if (!node.isNull())
    {
      if (node.isValueNode())
      {
        if (node.isNumber())
          return node.numberValue();
        if (node.isTextual())
          return node.textValue();
        if (node.isBoolean())
          return node.booleanValue();
      }
      else if (node.isObject())
        return node.toString();
    }
    return null;
  }

  @SuppressWarnings("incomplete-switch")
  private Object convert(FieldGroup event, FieldDefinition fieldDef, JsonNode node) throws FieldException
  {
    if (FieldType.Group.equals(fieldDef.getType()))
    {
      FieldGroup group = event.createFieldGroup(fieldDef.getName());
      populateGeoEvent(group, node, fieldDef.getChildren());
      return group;
    }
    else
    {
      Object value = getJsonNodeValue(node);
      if (value != null)
      {
        switch (fieldDef.getType())
        {
          case String:
            return Converter.convertToString(value);
          case Boolean:
            return Converter.convertToBoolean(value);
          case Date:
            // LOGGER.info("JsonInboundParser:convert:Date " +
            if (node.isTextual())
            {
              // LOGGER.info("JsonInboundParser:convert:Date is textual");

              // flightaware sent date in epoch seconds, make it milliseconds by appending "000"
              return (customDateFormat != null && !customDateFormat.trim().isEmpty()) ? DateUtil.convert(node.asText() + "000", customDateFormat) : DateUtil.convert(node.asText() + "000");
            }
            else if (node.isLong() || node.isInt())
            {
              // flightaware sent date in epoch seconds, convert it to milliseconds
              Long ms = (Long) value * 1000; 

              // LOGGER.info("JsonInboundParser:convert: " + value.toString() + " -> " + ms.toString());
              return DateUtil.convert(ms.toString());
            }
            break;
          case Double:
            return Converter.convertToDouble(value);
          case Float:
            return Converter.convertToFloat(value);
          case Integer:
            return Converter.convertToInteger(value);
          case Long:
            return Converter.convertToLong(value);
          case Short:
            return Converter.convertToShort(value);
          case Geometry:
            try
            {
              return GeometryUtil.fromJson(Converter.convertToString(value));
            }
            catch (Exception ex)
            {
              ;
            }
        }
      }
    }
    return null;
  }

  private GeoEventDefinition deriveGeoEventDefinition(JsonNode attributes) throws ConfigurationException
  {
    GeoEventDefinition geoEventDefinition = new DefaultGeoEventDefinition();
    geoEventDefinition.setName(geoEventDefinitionName);
    geoEventDefinition.setAccessType(AccessType.editable);

    String edOwner = uri.toString();
    geoEventDefinition.setOwner(edOwner);

    haveDate = false;
    haveGeometry = false;

    List<FieldDefinition> fieldDefinitions = generateFieldDefinitions(attributes, null);
    if (buildGeometryFromFields)
      fieldDefinitions.add(new DefaultFieldDefinition("geometry", FieldType.Geometry, "GEOMETRY"));

    geoEventDefinition.setFieldDefinitions(fieldDefinitions);
    return geoEventDefinition;
  }

  private List<FieldDefinition> generateFieldDefinitions(JsonNode attributes, List<FieldDefinition> workingList) throws ConfigurationException
  {
    List<FieldDefinition> fieldDefinitions = workingList;
    if (fieldDefinitions == null)
      fieldDefinitions = new ArrayList<FieldDefinition>();
    Iterator<String> fieldNames = attributes.fieldNames();
    while (fieldNames.hasNext())
    {
      String fieldName = fieldNames.next();

      if (listContainsField(fieldDefinitions, fieldName))
        continue;

      try
      {
        JsonNode field = attributes.get(fieldName);
        Iterator<JsonNode> arrayElements = null;
        FieldCardinality cardinality = FieldCardinality.One;
        if (field.isArray())
        {
          cardinality = FieldCardinality.Many;
          arrayElements = field.elements();
          if (arrayElements.hasNext())
            field = arrayElements.next();
          else
          {
            // If we're examining an array with no elements in it, we
            // have no idea what it should contain. But String is a pretty good guess.
            FieldDefinition fieldDef = new DefaultFieldDefinition(fieldName, FieldType.String);
            fieldDef.setCardinality(FieldCardinality.Many);
            fieldDefinitions.add(fieldDef);
            continue;
          }
        }
        FieldDefinition fieldDef = null;
        if (field.isObject())
        {
          if (looksLikeGeometry(field.toString()))
            fieldDef = makeGeometryFieldDef(fieldName, field);
          else
          {
            fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Group);
            List<FieldDefinition> childFieldDefinitions = generateFieldDefinitions(field, null);
            if (arrayElements != null)
            {
              while (arrayElements.hasNext())
                childFieldDefinitions = generateFieldDefinitions(arrayElements.next(), childFieldDefinitions);
            }

            for (FieldDefinition child : childFieldDefinitions)
              fieldDef.addChild(child);
          }
        }
        else if (field.isNumber())
        {
          fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Double);
        }
        else if (field.isBoolean())
        {
          fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Boolean);
        }
        else if (field.isTextual())
        {
          String textValue = field.asText();
          if (looksLikeADate(textValue))
          {
            if (haveDate)
            {
              fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Date);
            }
            else
            {
              fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Date, "TIME_START");
              haveDate = true;
            }
          }
          else if (looksLikeGeometry(textValue))
          {
            if (haveGeometry)
            {
              fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Geometry);
            }
            else
            {
              haveGeometry = true;
              fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Geometry, "GEOMETRY");
            }
          }
          else
            fieldDef = new DefaultFieldDefinition(fieldName, FieldType.String);
        }
        else if (field instanceof NullNode)
        {
          fieldDef = new DefaultFieldDefinition(fieldName, FieldType.String);
        }
        if (fieldDef != null)
        {
          fieldDef.setCardinality(cardinality);
          if(fieldName.equals(trackIdField))
          {
        	fieldDef.addTag("TRACK_ID");
          }
          fieldDefinitions.add(fieldDef);
        }
      }
      catch (ConfigurationException e)
      {
        continue;
      }
    }
    return fieldDefinitions;
  }

  private boolean listContainsField(List<FieldDefinition> fieldDefinitions, String fieldName)
  {
    for (FieldDefinition existingDefinition : fieldDefinitions)
    {
      if (existingDefinition.getName().equals(fieldName))
        return true;
    }
    return false;
  }

  private FieldDefinition makeGeometryFieldDef(String fieldName, JsonNode field) throws ConfigurationException
  {
    FieldDefinition fieldDef = null;
    if (haveGeometry)
    {
      fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Geometry);
    }
    else
    {
      haveGeometry = true;
      fieldDef = new DefaultFieldDefinition(fieldName, FieldType.Geometry, "GEOMETRY");
    }
    return fieldDef;
  }

  private boolean looksLikeGeometry(String field)
  {
    boolean canParseAsGeometry = false;
    try
    {
      MapGeometry geom = GeometryUtil.fromJson(field);
      if (geom.getGeometry() != null)
        canParseAsGeometry = true;
    }
    catch (IOException e)
    {
    }
    return canParseAsGeometry;
  }

  private boolean looksLikeADate(String value)
  {
    if (customDateFormat.isEmpty())
    {
      if (Pattern.matches("\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d.*", value))
        return true;
      if (Pattern.matches("\\d?\\d/\\d?\\d/\\d?\\d?\\d\\d \\d?\\d:\\d\\d:\\d\\d( [a,A,p,P][m,M])?", value))
        return true;
    }
    else
    {
      SimpleDateFormat sdf = new SimpleDateFormat(customDateFormat);
      try
      {
        Date datetest = sdf.parse(value);
        if (datetest != null)
          return true;
        return false;
      }
      catch (ParseException e)
      {
        return false;
      }
    }
    return false;
  }

  public JsonInboundParser setCreatingGeoEventDefinition(boolean creatingGeoEventDefinition)
  {
    this.creatingGeoEventDefinition = creatingGeoEventDefinition;
    return this;
  }

  public JsonInboundParser setGeoEventDefinitionName(String geoEventDefinitionName)
  {
    this.geoEventDefinitionName = geoEventDefinitionName;
    return this;
  }

  public JsonInboundParser setBuildGeometryFromFields(boolean buildGeometryFromFields)
  {
    this.buildGeometryFromFields = buildGeometryFromFields;
    return this;
  }

  public JsonInboundParser setxGeometryField(String xGeometryField)
  {
    this.xGeometryField = xGeometryField;
    return this;
  }

  public JsonInboundParser setyGeometryField(String yGeometryField)
  {
    this.yGeometryField = yGeometryField;
    return this;
  }

  public JsonInboundParser setzGeometryField(String zGeometryField)
  {
    this.zGeometryField = zGeometryField;
    return this;
  }

  public JsonInboundParser setWkidGeometryField(String wkidGeometryField)
  {
    this.wkidGeometryField = wkidGeometryField;
    return this;
  }

  public JsonInboundParser setWkTextGeometryField(String wkTextGeometryField)
  {
    this.wkTextGeometryField = wkTextGeometryField;
    return this;
  }

  public JsonInboundParser setCustomDateFormat(String customDateFormat)
  {
    this.customDateFormat = customDateFormat;
    return this;
  }

  public JsonInboundParser setGeoEventCreator(GeoEventCreator geoEventCreator)
  {
    this.geoEventCreator = geoEventCreator;
    return this;
  }

  public JsonInboundParser setUri(Uri uri)
  {
    this.uri = uri;
    return this;
  }
}
