package com.esri.json.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 * Enumerates records from an Esri Unenclosed JSON file
 * 
 */
public class UnenclosedJsonRecordReader implements RecordReader<LongWritable, Text>{
	static final Log LOG = LogFactory.getLog(UnenclosedJsonRecordReader.class.getName());
	
	private BufferedReader inputReader;
	private FileSplit fileSplit;
	
	long fileLength;
	
	int readerPosition = 0;
	
	public UnenclosedJsonRecordReader(InputSplit split, Configuration conf) throws IOException
	{
		fileSplit = (FileSplit)split;
		fileLength = fileSplit.getLength();
		
		Path filePath = fileSplit.getPath();
		
		FileSystem fs = filePath.getFileSystem(conf);
		
		inputReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
	}
	
	@Override
	public void close() throws IOException {
		if (inputReader != null)
			inputReader.close();
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return readerPosition;
	}

	@Override
	public float getProgress() throws IOException {
		return (float)readerPosition/fileLength;
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		/*
		 * NOTE : we are not using a JSONParser, so this will not validate JSON structure aside from correct counts of '{' and '}'
		 * 
		 * The JSON will look like this (white-space ignored)
		 * 
		 * { // start record 1
		 * 	"attrubites" : {}
		 *  "geometry" : {}
		 * } // end record 1
		 * { // start record 2
		 * 	"attrubites" : {}
		 *  "geometry" : {}
		 * } // end record 2
		 * 
		 * We will count '{' and '}' to find the beginning and end of each record, while ignoring braces in string literals
		 */
		int paren_depth = 0;
		int chr = 0;
		char lit_char = 0;
		
		boolean first_paren_found = false;
		
		StringBuilder sb = new StringBuilder(2000);
		
		while (paren_depth > 0 || !first_paren_found)
		{
			chr = inputReader.read();
			readerPosition++;
			
			if (chr < 0){
				if (first_paren_found){
					// last record was invalid
					LOG.error("Parsing error : EOF occured before record ended");
				}
				return false;
			}
			
			switch (chr)
			{
			case '"':
				if (lit_char == '"')
				{
					lit_char = 0; // mark end literal (double-quote)
				} 
				else if (lit_char == 0)
				{
					lit_char = '"'; // mark start literal (double quote)
				} 
				// ignored because we found a " inside a ' ' block quote
				break;
			case '\'':
				if (lit_char == '\'')
				{
					lit_char = 0; // mark end literal (single-quote)
				} 
				else if (lit_char == 0)
				{
					lit_char = '\''; // mark start literal (single quote)
				} 
				// ignored because we found a ' inside a " " block quote
				break;
			case '{':
				if (lit_char == 0) // not in string literal, so increase paren depth
				{
					paren_depth++;
					first_paren_found = true;
					key.set(readerPosition); // set record key to the char offset of the first '{'
				}
				break;
			case '}':
				if (lit_char == 0) // not in string literal, so decrease paren depth
				{
					paren_depth--;
				}
				break;
			}
			
			if (paren_depth < 0){
				// found more '}'s than we did '{'s
				LOG.error("Parsing error : unmatched '}' in record");
				return false;
			}
			
			if (first_paren_found){
				sb.append((char)chr);
			}
		}
		
		// no '{' found before EOF.  Not an error as this could mean that there is extra white-space at the end
		if (!first_paren_found){
			return false;
		}
		
		value.set(sb.toString());
		return true;
	}

}
