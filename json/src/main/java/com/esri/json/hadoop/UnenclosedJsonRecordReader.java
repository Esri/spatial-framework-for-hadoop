/* *
package com.esri.json.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
 * Obsolete renamed to and superseded by UnenclosedEsriJsonRecordReader
@Deprecated in v1.2
public class UnenclosedJsonRecordReader extends UnenclosedEsriJsonRecordReader {
	public UnenclosedJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}
	public UnenclosedJsonRecordReader(org.apache.hadoop.mapred.InputSplit split,
									  Configuration conf) throws IOException {
		super(split, conf);
	}
}
 * */
