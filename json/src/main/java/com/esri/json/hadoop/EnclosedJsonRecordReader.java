package com.esri.json.hadoop;

import java.io.IOException;

/**
 * @deprecated superseded by EnclosedEsriJsonRecordReader
 */
@Deprecated
public class EnclosedJsonRecordReader extends UnenclosedEsriJsonRecordReader {
	public EnclosedJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}
}
