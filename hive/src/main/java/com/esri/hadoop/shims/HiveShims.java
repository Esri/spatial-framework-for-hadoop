package com.esri.hadoop.shims;

public class HiveShims {

	/**
	 * This class is supplied for compatibility between Hive versions. 
	 * At 10.0 the serde constants were moved to another package.  Also,
	 * at 11.0 the previous class will be re-added for backwards
	 * compatibility, but deprecated
	 *
	 */
	public static class serdeConstants {
		public static final String LIST_COLUMNS;
		public static final String LIST_COLUMN_TYPES;
		
		static {
			Class<?> clazz = null;
			
			try {
				// Hive 10 and above constants
				clazz = Class.forName("org.apache.hadoop.hive.serde.serdeConstants");
			} catch (ClassNotFoundException e) {
				try {
					// Hive 9 and below constants
					clazz = Class.forName("org.apache.hadoop.hive.serde.Constants");
				} catch (ClassNotFoundException e1) {
					// not much we can do here
				}
			}

			LIST_COLUMNS = getAsStringOrNull(clazz, "LIST_COLUMNS");
			LIST_COLUMN_TYPES = getAsStringOrNull(clazz, "LIST_COLUMN_TYPES");
		}
		
		static String getAsStringOrNull(Class<?> clazz, String constant) {
			try {
				return (String) clazz.getField(constant).get(null);
			} catch (Exception e) {
				return null;
			} 
		}
	}
}
