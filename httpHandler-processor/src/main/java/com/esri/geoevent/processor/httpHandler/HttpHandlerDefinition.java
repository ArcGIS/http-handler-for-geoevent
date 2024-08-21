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

import java.util.ArrayList;
import java.util.List;

import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class HttpHandlerDefinition extends GeoEventProcessorDefinitionBase
{
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(HttpHandlerDefinition.class);

  public HttpHandlerDefinition()
  {
    try
    {
      List<LabeledValue> methodAllowedValues = new ArrayList<>();
      methodAllowedValues.add(new LabeledValue("GET", "GET"));
      methodAllowedValues.add(new LabeledValue("POST", "POST"));
      // methodAllowedValues.add(new LabeledValue("PUT", "PUT"));
      // methodAllowedValues.add(new LabeledValue("UPDATE", "UPDATE"));
      // methodAllowedValues.add(new LabeledValue("DELETE", "DELETE"));

      propertyDefinitions.put("method", new PropertyDefinition("method", PropertyType.String, "GET", "Method", "HTTP method to be used", false, false, methodAllowedValues));

      List<LabeledValue> formatAllowedValues = new ArrayList<>();
      formatAllowedValues.add(new LabeledValue("Json", "json"));
      formatAllowedValues.add(new LabeledValue("XML", "xml"));
      formatAllowedValues.add(new LabeledValue("CSV", "csv"));
      propertyDefinitions.put("responseFormat", new PropertyDefinition("responseFormat", PropertyType.String, "json", "Response Format", "Response Format", true, false, formatAllowedValues));
      propertyDefinitions.put("fieldSeparator", new PropertyDefinition("fieldSeparator", PropertyType.String, ",", "Field Separator", "Field Separator", "responseFormat=csv", false, false));

      List<LabeledValue> modeAllowedValues = new ArrayList<>();
      formatAllowedValues.add(new LabeledValue("Server", "SERVER"));
      formatAllowedValues.add(new LabeledValue("Client", "CLIENT"));
      propertyDefinitions.put("mode", new PropertyDefinition("Mode", PropertyType.String, "CLIENT", "Mode", "Mode", true, false, modeAllowedValues));
      
      propertyDefinitions.put("clientURL", new PropertyDefinition("clientURL", PropertyType.String, "", "URL", "URL composed from fields in the format http://host/{field1}/folder/{field2}?value1={field3}&value2={$lastPollingDateTime}&value3={$currentDateTime}. Processor-defined (epoch-millisecond) time fields are $lastPollingDateTime and $currentDateTime", false, false));
      propertyDefinitions.put("JsonObjectName", new PropertyDefinition("JsonObjectName", PropertyType.String, "", "Objectname", "Tag to use as object name", false, false));
      propertyDefinitions.put("httpMethod", new PropertyDefinition("httpMethod", PropertyType.String, "Get", "HTTP method", "HTTP method", true, false, methodAllowedValues));
      propertyDefinitions.put("CreateGeoEventDefinition", new PropertyDefinition("CreateGeoEventDefinition", PropertyType.Boolean, true, "Create New GeoEvent Definition", "Create New GeoEvent Definition", false, false));
      propertyDefinitions.put("NewGeoEventDefinitionName", new PropertyDefinition("NewGeoEventDefinitionName", PropertyType.String, "NewGeoEventDefinition", "New GeoEvent Definition Name", "New GeoEvent Definition Name", "CreateGeoEventDefinition=true", false, false));
      propertyDefinitions.put("ExistingGeoEventDefinitionName", new PropertyDefinition("ExistingGeoEventDefinitionName", PropertyType.String, "ExistingGeoEventDefinition", "Existing GeoEvent Definition Name", "Existing GeoEvent Definition Name", "CreateGeoEventDefinition=false", false, false));

      propertyDefinitions.put("headers", new PropertyDefinition("headers", PropertyType.String, "", "Headers", "HTTP headers name:value with | separator (params after :{field})", false, false));
      propertyDefinitions.put("body", new PropertyDefinition("body", PropertyType.String, "", "Body", "HTTP body", "httpMethod=POST,httpMethod=PUT", false, false));

      propertyDefinitions.put("TrackIdField", new PropertyDefinition("TrackIdField", PropertyType.String, "", "TrackId Field", "TrackId Field", false, false));
      
      propertyDefinitions.put("BuildGeometryFromFields", new PropertyDefinition("BuildGeometryFromFields", PropertyType.Boolean, true, "Build Geometry From Fields", "Build geometry from fields", false, false));
      propertyDefinitions.put("XGeometryField", new PropertyDefinition("XGeometryField", PropertyType.String, "Longitude", "X Geometry Field", "X Geometry Field", "BuildGeometryFromFields=true", false, false));
      propertyDefinitions.put("YGeometryField", new PropertyDefinition("YGeometryField", PropertyType.String, "Latitude", "Y Geometry Field", "Y Geometry Field", "BuildGeometryFromFields=true", false, false));
      propertyDefinitions.put("ZGeometryField", new PropertyDefinition("ZGeometryField", PropertyType.String, "", "Z Geometry Field", "Z Geometry Field", "BuildGeometryFromFields=true", false, false));
      propertyDefinitions.put("WKIDGeometryField", new PropertyDefinition("WKIDGeometryField", PropertyType.String, "4326", "WKID Filed or Value", "WKID field or value", false, false));
      propertyDefinitions.put("CustomDateFormat", new PropertyDefinition("CustomDateFormat", PropertyType.String, "", "Custom Date Format", "Custom Date Format", false, false));
      
      propertyDefinitions.put("frequency", new PropertyDefinition("frequency", PropertyType.Integer, "5", "Frequency", "Frequency in seconds", false, false));
      propertyDefinitions.put("httpTimeoutValue", new PropertyDefinition("httpTimeoutValue", PropertyType.Integer, "5", "Http Timeout Value", "Http Timeout Value", false, false));
      propertyDefinitions.put("historicalTimespanSeconds", new PropertyDefinition("historicalTimespanSeconds", PropertyType.Integer, "5", "Historical Timespan (seconds)", "Historical Timespan in seconds for calculate initial value of the processor-defined field $lastPollingDateTime", false, false));
      propertyDefinitions.put("useEpochMilliseconds", new PropertyDefinition("useEpochMilliseconds", PropertyType.Boolean, false, "Use Epoch Milliseconds", "Use Epoch Milliseconds. The default is epoch seconds", false, false));
    }
    catch (Exception error)
    {
      LOGGER.error("INIT_ERROR", error.getMessage());
      LOGGER.info(error.getMessage(), error);
    }
  }

  @Override
  public String getName()
  {
    return "HttpHandler";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.processor";
  }

  @Override
  public String getVersion()
  {
    return "11.3.0";
  }

  @Override
  public String getLabel()
  {
	return "Http Handlerv2";
    //return "${com.esri.geoevent.processor.httpHandler-processor.PROCESSOR_LABEL}";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.processor.httpHandler-processor.PROCESSOR_DESC}";
  }
}
