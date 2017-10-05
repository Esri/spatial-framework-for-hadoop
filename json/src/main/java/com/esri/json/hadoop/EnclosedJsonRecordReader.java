/* *
package com.esri.json.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
 * Obsolete - renamed to and superseded by EnclosedEsriJsonRecordReader
@Deprecated in v1.2
public class EnclosedJsonRecordReader extends EnclosedEsriJsonRecordReader {
	public EnclosedJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}
	public EnclosedJsonRecordReader(org.apache.hadoop.mapred.InputSplit split,
									  Configuration conf) throws IOException {
		super(split, conf);
	}
}
* */
