package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorEquals;
import com.esri.core.geometry.OperatorSimpleRelation;

@Description(
	name = "ST_Equals",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 equals geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,1, 0,0)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,0, 0,1)) from src LIMIT 1;  -- return false\n"	
	)
public class ST_Equals extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorEquals.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s equals %s", args[0], args[1]);
	}
}
