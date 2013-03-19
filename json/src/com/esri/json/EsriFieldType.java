package com.esri.json;

/**
 * 
 * Enumeration of Esri field types.  These are not upper cased as they are a direct
 * string representation of what would is in the JSON
 */
public enum EsriFieldType {
	esriFieldTypeSmallInteger, 
	esriFieldTypeInteger,
	esriFieldTypeSingle,
	esriFieldTypeDouble,
	esriFieldTypeString,
	esriFieldTypeDate,
	esriFieldTypeOID,
	esriFieldTypeGeometry,
	esriFieldTypeBlob,
	esriFieldTypeRaster,
	esriFieldTypeGUID,
	esriFieldTypeGlobalID,
	esriFieldTypeXML,
}