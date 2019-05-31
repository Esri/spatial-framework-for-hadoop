package com.esri.hadoop.hive;

import java.util.EnumSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.esri.core.geometry.ogc.OGCPoint;

@Description(
		name = "ST_Bin",
		value = "_FUNC_(binsize, point) - return bin ID for given point\n")
public class ST_Bin extends GenericUDF {

	private transient HiveGeometryOIHelper geomHelper;
	private transient boolean binSizeIsConstant;
	private transient PrimitiveObjectInspector oiBinSize;
	private transient BinUtils bins;
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] OIs)
			throws UDFArgumentException {
		
		if (OIs.length != 2) {
			throw new UDFArgumentException("Function takes exactly 2 arguments");
		}

		if (OIs[0].getCategory() != Category.PRIMITIVE) {
			throw new UDFArgumentException("Argument 0 must be a number - got: " + OIs[0].getCategory());
		}

		oiBinSize = (PrimitiveObjectInspector)OIs[0];
		if (!EnumSet.of(PrimitiveCategory.DECIMAL,PrimitiveCategory.DOUBLE,PrimitiveCategory.INT,PrimitiveCategory.LONG,PrimitiveCategory.SHORT, PrimitiveCategory.FLOAT).contains(oiBinSize.getPrimitiveCategory())) {
			throw new UDFArgumentException("Argument 0 must be a number - got: " + oiBinSize.getPrimitiveCategory());
		}

		geomHelper = HiveGeometryOIHelper.create(OIs[1], 1);
		binSizeIsConstant = ObjectInspectorUtils.isConstantObjectInspector(OIs[0]);

		return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		double binSize = PrimitiveObjectInspectorUtils.getDouble(args[0].get(), oiBinSize);

		if (!binSizeIsConstant || bins == null) {
			bins = new BinUtils(binSize);
		} 

		OGCPoint point = geomHelper.getPoint(args);

		if (point == null) {
			return null;
		}

		return bins.getId(point.X(), point.Y());
	}

	@Override
	public String getDisplayString(String[] args) {
		assert(args.length == 2);
		return String.format("st_bin(%s,%s)", args[0], args[1]);
	}

}
