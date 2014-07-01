package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorSimpleRelation;

@Description(
	name = "ST_Intersects",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 intersects geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(ST_LineString(2,0, 2,3), ST_Polygon(1,1, 4,1, 4,4, 1,4))) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(ST_LineString(8,7, 7,8), ST_Polygon(1,1, 4,1, 4,4, 1,4)) from src LIMIT 1;  -- return false\n"	
	)

public class ST_Intersects extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorIntersects.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s intersects %s", args[0], args[1]);
	}
}
