package com.esri.json.hadoop;

import java.io.IOException;

/**
 * @deprecated superseded by UnenclosedEsriJsonRecordReader
 */
@Deprecated
public class UnenclosedJsonRecordReader extends UnenclosedEsriJsonRecordReader {
	public UnenclosedJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}
}
