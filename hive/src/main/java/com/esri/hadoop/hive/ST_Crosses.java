package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorCrosses;
import com.esri.core.geometry.OperatorSimpleRelation;

@Description(
	name = "ST_Crosses",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 crosses geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,0, 0,1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_linestring(2,0, 2,3), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_linestring(0,2, 0,1), ST_linestring(2,0, 1,0)) from src LIMIT 1;  -- return false"	
	)
public class ST_Crosses extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorCrosses.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s crosses %s", args[0], args[1]);
	}
}
