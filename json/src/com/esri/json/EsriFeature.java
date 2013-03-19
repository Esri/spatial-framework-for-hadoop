package com.esri.json;
import java.util.Map;

import com.esri.core.geometry.Geometry;


public class EsriFeature {
	/**
	 * Map of attributes
	 */
	public Map<String, Object> attributes;
	
	/**
	 * Geometry associated with this feature
	 */
	public Geometry geometry;
}
