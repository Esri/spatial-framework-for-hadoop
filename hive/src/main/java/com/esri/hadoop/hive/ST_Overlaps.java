package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorSimpleRelation;

@Description(
	name = "ST_Overlaps",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 overlaps geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_polygon(2,0, 2,3, 3,0), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return false"	
	)

public class ST_Overlaps extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorOverlaps.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s overlaps %s", args[0], args[1]);
	}
}
