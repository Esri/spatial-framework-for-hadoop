package com.esri.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EsriField {
	/**
	 * Actual name of the field
	 */
	public String name;
	
	/**
	 * Field value type (i.e. esriFieldTypeString)
	 */
	public EsriFieldType type;
	
	/**
	 * Aliased name of the field
	 */
	public String alias;
	
	/**
	 * Field maximum length (for value types like esriFieldTypeString)
	 */
	public int length;
}
