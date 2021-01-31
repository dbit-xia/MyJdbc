package node;

//import java.io.File;
import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
//import java.io.OutputStream;
//import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
//import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
//import com.fasterxml.jackson.core.*;
//import com.fasterxml.jackson.databind.*;

import cn.hutool.poi.excel.BigExcelWriter;
import cn.hutool.poi.excel.ExcelUtil;
import org.apache.poi.ss.usermodel.Cell;


//class Car {
//	public String brand;
//	public int doors;
//}

//import netscape.javascript.JSObject;

public class jdbc {

	public static void main(String[] args) throws SQLException, IOException {

		System.out.println(encode("\"\\12	3夏子aa夏ABC&://-+ 1\n\r23","UTF_16BE"));

//		String jsonArray = "[{\"brand\":\"ford\"}, {\"brand\":\"Fiat\"}]";

//		ObjectMapper objectMapper = new ObjectMapper();
//
//		Car[] cars2 = objectMapper.readValue(jsonArray, Car[].class);
//		String carJson =
//		        "[[\"123\",null,456,null],[\"123\",null,456]]";
//
//		JsonFactory factory = new JsonFactory();
//		JsonParser  parser  = factory.createParser(carJson);
//		parser.enable(JsonParser.Feature.ALLOW_TRAILING_COMMA);
//		parser.enable(JsonParser.Feature.ALLOW_MISSING_VALUES);

//		int i=0;
////		Car car = new Car();
//		while(!parser.isClosed()){
//		    JsonToken jsonToken = parser.nextToken();
////		    System.out.println(parser.hasCurrentToken());
////		    System.out.println(parser.getValueAsString());
//		    i++;
//		}

//		factory.
//		System.out.println(i);
//		System.out.println("car.doors = " + car.doors);

//		JSONArray value = JSON.parseArray("[[\"123\",undefined,456]]");
//		System.out.println(((JSONArray) value.get(0)).get(1)==null);// new Date()为获取当前系统时间
//
//		JsonArray value = Json.parse("[1,null,2]").asArray();
//		System.out.println(value);// new Date()为获取当前系统时间
//		try {
//			System.in.read();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

//		int a=1;
//		Object temp=a;
//		if (temp.getClass()==java.lang.Integer.class){
//			System.out.print("Int");
//			temp=((Integer) temp).doubleValue();
//		}
//		System.out.print(a);

		// TODO Auto-generated method stub
//		JSObject object = JSObject
//				json_encode;
//		object.setMember("a", 123);
//		System.out.println(object.toString());
//		object.

//		(2)调用JSONObject的put方法向object中传值
//		[java] view plain copy
//		object.put("name", "zhangsan");
//		object.put("age", 20);
//		JSONArray skills = new JSONArray();
//		skills.put("java");
//		skills.put("php");
//		object.put("skills", skills);

//		System.out.println(object);
	}

    static BitSet dontNeedEncoding;
    static final int caseDiff = ('a' - 'A');
    static String dfltEncName = null;

    static {

	/* The list of characters that are not encoded has been
	 * determined as follows:
	 *
	 * RFC 2396 states:
	 * -----
	 * Data characters that are allowed in a URI but do not have a
	 * reserved purpose are called unreserved.  These include upper
	 * and lower case letters, decimal digits, and a limited set of
	 * punctuation marks and symbols.
	 *
	 * unreserved  = alphanum | mark
	 *
	 * mark        = "-" | "_" | "." | "!" | "~" | "*" | "'" | "(" | ")"
	 *
	 * Unreserved characters can be escaped without changing the
	 * semantics of the URI, but this should not be done unless the
	 * URI is being used in a context that does not allow the
	 * unescaped character to appear.
	 * -----
	 *
	 * It appears that both Netscape and Internet Explorer escape
	 * all special characters from this list with the exception
	 * of "-", "_", ".", "*". While it is not clear why they are
	 * escaping the other characters, perhaps it is safest to
	 * assume that there might be contexts in which the others
	 * are unsafe if not escaped. Therefore, we will use the same
	 * list. It is also noteworthy that this is consistent with
	 * O'Reilly's "HTML: The Definitive Guide" (page 164).
	 *
	 * As a last note, Intenet Explorer does not encode the "@"
	 * character which is clearly not unreserved according to the
	 * RFC. We are being consistent with the RFC in this matter,
	 * as is Netscape.
	 *
	 */

	dontNeedEncoding = new BitSet(256);
	int i;
	for (i = 'a'; i <= 'z'; i++) {
	    dontNeedEncoding.set(i);
	}
	for (i = 'A'; i <= 'Z'; i++) {
	    dontNeedEncoding.set(i);
	}
	for (i = '0'; i <= '9'; i++) {
	    dontNeedEncoding.set(i);
	}
	dontNeedEncoding.set(' ');
	dontNeedEncoding.set('~');
	dontNeedEncoding.set('`');
	dontNeedEncoding.set('!');
	dontNeedEncoding.set('@');
	dontNeedEncoding.set('#');
	dontNeedEncoding.set('$');
	dontNeedEncoding.set('%');
	dontNeedEncoding.set('^');
	dontNeedEncoding.set('&');
	dontNeedEncoding.set('*');
	dontNeedEncoding.set('(');
	dontNeedEncoding.set(')');
	dontNeedEncoding.set('-');
	dontNeedEncoding.set('_');
	dontNeedEncoding.set('+');
	dontNeedEncoding.set('=');
	dontNeedEncoding.set('|');
	dontNeedEncoding.set('{');
	dontNeedEncoding.set('}');
	dontNeedEncoding.set('[');
	dontNeedEncoding.set(']');
	dontNeedEncoding.set(':');
	dontNeedEncoding.set(';');
	dontNeedEncoding.set('<');
	dontNeedEncoding.set('>');
	dontNeedEncoding.set(',');
	dontNeedEncoding.set('.');
	dontNeedEncoding.set('/');
	dontNeedEncoding.set('?');
	dontNeedEncoding.set('\'');
//	dontNeedEncoding.set('\t');

//    	dfltEncName = (String)AccessController.doPrivileged (
//	    new GetPropertyAction("file.encoding")
//    	);
    }

    /**
     * You can't call the constructor.
     */
//    private JSONEncoder() { }

    /**
     * Translates a string into <code>x-www-form-urlencoded</code>
     * format. This method uses the platform's default encoding
     * as the encoding scheme to obtain the bytes for unsafe characters.
     *
     * @param   s   <code>String</code> to be translated.
     * @deprecated The resulting string may vary depending on the platform's
     *             default encoding. Instead, use the encode(String,String)
     *             method to specify the encoding.
     * @return  the translated <code>String</code>.
     */
//    @Deprecated
//    public static String encode(String s) {
//
//		String str = null;
//
//		try {
//		    str = encode(s, dfltEncName);
//		} catch (UnsupportedEncodingException e) {
//		    // The system should always have the platform default
//		}
//
//		return str;
//    }

    /**
     * Translates a string into <code>application/x-www-form-urlencoded</code>
     * format using a specific encoding scheme. This method uses the
     * supplied encoding scheme to obtain the bytes for unsafe
     * characters.
     * <p>
     * <em><strong>Note:</strong> The <a href=
     * "http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars">
     * World Wide Web Consortium Recommendation</a> states that
     * UTF-8 should be used. Not doing so may introduce
     * incompatibilites.</em>
     *
     * @param   s   <code>String</code> to be translated.
     * @param   enc   The name of a supported
     *    <a href="../lang/package-summary.html#charenc">character
     *    encoding</a>.
     * @return  the translated <code>String</code>.
     * @exception  UnsupportedEncodingException
     *             If the named encoding is not supported
     * @see URLDecoder#decode(java.lang.String, java.lang.String)
     * @since 1.4
     */
    public static String encode(String s, String enc) throws UnsupportedEncodingException {

	boolean needToChange = false;
        StringBuffer out = new StringBuffer(s.length());
	Charset charset;
	CharArrayWriter charArrayWriter = new CharArrayWriter();

	if (enc == null)
	    throw new NullPointerException("charsetName");

	try {
	    charset = Charset.forName(enc);
	} catch (IllegalCharsetNameException e) {
            throw new UnsupportedEncodingException(enc);
        } catch (UnsupportedCharsetException e) {
	    throw new UnsupportedEncodingException(enc);
	}

	for (int i = 0; i < s.length();) {
	    int c = (int) s.charAt(i);
	    //System.out.println("Examining character: " + c);
	    if (dontNeedEncoding.get(c)) {
//			if (c == ' ') {
//			    c = '+';
//			    needToChange = true;
//			}
			//System.out.println("Storing: " + c);
			out.append((char)c);
			i++;
	    } else {
		// convert to external encoding before hex conversion
		do {
		    charArrayWriter.write(c);
		    /*
		     * If this character represents the start of a Unicode
		     * surrogate pair, then pass in two characters. It's not
		     * clear what should be done if a bytes reserved in the
		     * surrogate pairs range occurs outside of a legal
		     * surrogate pair. For now, just treat it as if it were
		     * any other character.
		     */
		    if (c >= 0xD800 && c <= 0xDBFF) {
			/*
			  System.out.println(Integer.toHexString(c)
			  + " is high surrogate");
			*/
			if ( (i+1) < s.length()) {
			    int d = (int) s.charAt(i+1);
			    /*
			      System.out.println("\tExamining "
			      + Integer.toHexString(d));
			    */
			    if (d >= 0xDC00 && d <= 0xDFFF) {
				/*
				  System.out.println("\t"
				  + Integer.toHexString(d)
				  + " is low surrogate");
				*/
			        charArrayWriter.write(d);
				i++;
			    }
			}
		    }
		    i++;
		} while (i < s.length() && !dontNeedEncoding.get((c = (int) s.charAt(i))));

		charArrayWriter.flush();
		String str = new String(charArrayWriter.toCharArray());
		byte[] ba = str.getBytes(charset);
		for (int j = 0; j < ba.length; j++) {
			if (j%2==0 ) out.append("\\u");
		    char ch = Character.forDigit((ba[j] >> 4) & 0xF, 16);
		    // converting to use uppercase letter as part of
		    // the hex value if ch is a letter.
		    if (Character.isLetter(ch)) {
			ch -= caseDiff;
		    }
		    out.append(ch);
		    ch = Character.forDigit(ba[j] & 0xF, 16);
		    if (Character.isLetter(ch)) {
			ch -= caseDiff;
		    }
		    out.append(ch);
		}
		charArrayWriter.reset();
		needToChange = true;
	    }
	}

	return (needToChange? out.toString() : s);
    }

	public static int hello(Object[] data) throws IOException{
		System.out.println(1);
		System.out.write(2);
		System.in.read("123".getBytes());
//		System.console();
//		System.out.close();

		data[0]="123\n";
		return 1;
	}

	@SuppressWarnings("unchecked")
	public static Object[][] getArray(int colcount){
		System.out.println(colcount);
		ArrayList<ArrayList<?>> resultList=new ArrayList<ArrayList<?>>();

		resultList.add(new ArrayList<Integer>());
		resultList.add(new ArrayList<String>());

		((ArrayList<Integer>)resultList.get(0)).add(1);
		((ArrayList<String>)resultList.get(0)).add("a");
		((ArrayList<Integer>)resultList.get(1)).add(2);
		((ArrayList<String>)resultList.get(1)).add("b");


		Object[][] outTable = new Object[((ArrayList<String>)resultList.get(1)).size()][2];

//		((ArrayList<String>)resultList.get(1)).toArray(outTable);
		outTable[0][0]=1.01;
		outTable[0][1]=2.01;
		outTable[1][0]="a";
		outTable[1][1]="b";
		return outTable;
	}

	@SuppressWarnings("unchecked")
	public static Object[][] rs2table(ResultSet rs) throws SQLException { //

//		Object[][] outTable = new Object[2][2];
//		System.out.println(fieldsCount);
//		return outTable;

		ResultSetMetaData rsmd = rs.getMetaData();//
		int fieldsCount=rsmd.getColumnCount();

		ArrayList<ArrayList<?>> resultList=new ArrayList<ArrayList<?>>();

		for (int i=0;i<fieldsCount;i++){
			resultList.add(new ArrayList<Object>());
		}

		while(rs.next()==true){
			for (int i=0;i<fieldsCount;i++){
				((ArrayList<Object>)resultList.get(i)).add(rs.getObject(i + 1));
			}
        }

		int rowCount=resultList.get(0).size();
		if (rowCount==0) return null; //
		Object[][] outTable = new Object[rowCount][fieldsCount];

		ArrayList<Object> temp;
		for (int i=0;i<fieldsCount;i++){
			temp=(ArrayList<Object>)resultList.get(i);
//			System.out.println(rsmd.getColumnTypeName(i+1));
			switch (rsmd.getColumnTypeName(i+1)){
				case "decimal":
				case "numeric":
				case "numeric identity":
					for (int r=0;r<rowCount;r++){
						outTable[r][i]=temp.get(r)==null?null:((BigDecimal)temp.get(r)).floatValue();
					}
					break;
				case "datetime":
					for (int r=0;r<rowCount;r++){
						outTable[r][i]=temp.get(r)==null?null:temp.get(r).toString();
					}
					break;
				default:
					for (int r=0;r<rowCount;r++){
						outTable[r][i]=(Object)temp.get(r);
					}
			}
		}

//		System.out.println(outTable);

		return outTable;
	}

    public static Object[] rs2table2(ResultSet rs) throws SQLException { //

        ResultSetMetaData rsmd = rs.getMetaData();//
        int fieldsCount=rsmd.getColumnCount();

        ArrayList<Object> resultList=new ArrayList<Object>();

        //for (int i=0;i<fieldsCount;i++){
        //	resultList.add(new ArrayList<Object>());
        //}
        Object[] temp;
        Object value;

        long starTime=System.currentTimeMillis();

        while(rs.next()==true){
            temp=new Object[fieldsCount];
            for (int i=0;i<fieldsCount;i++){
                value=rs.getObject(i + 1);
                switch (rsmd.getColumnTypeName(i+1)){
                    case "decimal":
                    case "numeric":
                    case "numeric identity":
                        //for (int r=0;r<rowCount;r++){
                            temp[i]=(value==null?null:((BigDecimal)value).floatValue());
                        //}
                        break;
                    case "datetime":
                        //for (int r=0;r<rowCount;r++){
                            temp[i]=(value==null?null:value.toString());
                        //}
                        break;
                    default:
                        //for (int r=0;r<rowCount;r++){
                            temp[i]=(value);
                        //}
                }
            }
            resultList.add(temp);
        }

        int rowCount=resultList.size();
        if (rowCount==0) return null; //

        Object[] outTable;
        outTable = new Object[rowCount];

        //ArrayList<Object> temp;
        for (int r=0;r<rowCount;r++){
            outTable[r]=resultList.get(r);
            //temp=(ArrayList<Object>)resultList.get(r);
            //for (int i=0;i<fieldsCount;i++){
//			System.out.println(rsmd.getColumnTypeName(i+1));
            //    outTable[r][i]=(Object)temp.get(i);

            //}
        }


		long endTime=System.currentTimeMillis();
		long Time=endTime-starTime;
		System.out.println(Time);

        return outTable;
    }

    public static String rs2table3(ResultSet rs,String option) throws SQLException, IOException { //
//    	System.out.println("rs2table3");
    	JsonObject optionObject=Json.parse(option).asObject();
    	String filename=optionObject.getString("filename", "");

    	FileWriter fileWriter = null;
    	if (filename.isEmpty()==false) fileWriter = new FileWriter(filename);

        ResultSetMetaData rsmd = rs.getMetaData();//
        int fieldsCount=rsmd.getColumnCount();

        Object value="";

        int rowCount=0;
		StringBuilder sb=new StringBuilder();

//		Object[][] metadata=new Object[2][fieldsCount];

		//[[列名],[类型],[数据]]
		sb.append("[[");
	    for (int i = 1; i <= fieldsCount; i++) {
	    	if (i>1) sb.append(',');
	    	sb.append("\""+rsmd.getColumnLabel(i)+"\"");
	    }
	    sb.append("],[");
	    for (int i = 1; i <= fieldsCount; i++) {
	    	if (i>1) sb.append(',');
	    	sb.append(String.valueOf(rsmd.getColumnType(i)));
	    }
	    sb.append("],");

	    StringBuilder rowsBuilder=new StringBuilder();
		if (rs.next()==true) {
			rowsBuilder.append("[");
		} else {
			sb.append("null]");
			return sb.toString();
		}
        do{
        	rowCount++;
        	if (rowCount==1){
        		rowsBuilder.append("[");
        	}else{
        		rowsBuilder.append(",[");
        	}

            for (int i=0;i<fieldsCount;i++){
            	if (i!=0) rowsBuilder.append(",");
                value=rs.getObject(i + 1);
                if (value==null){
                	rowsBuilder.append("null");
                }else{
                	//System.out.print(rsmd.getColumnTypeName(i+1)+',');
	                switch (rsmd.getColumnTypeName(i+1).toLowerCase()){
	                	case "varbinary":
	                		rowsBuilder.append("0");
		                    break;
		                case "datetime":case "date":case "time":case "timestamp":case "timestamptz":
		                	rowsBuilder.append("\""+value.toString()+"\"");
		                    break;
		                case "varchar":case "text":case "nvarchar":case "clob":case "bpchar":case "name":
                            value=value.toString(); //.replaceFirst("\\s+$", "");
                            if (value.equals(" ")) value=""; //ASE Tds空字符串会返回空格,所以特殊处理!!!
//                            value=((String)value).replace("\\","\\\\");
////		                	value=((String)value).replace("{","\\{");
////		                	value=((String)value).replace("[","\\[");
////		                	value=((String)value).replace("}","\\}");
////		                	value=((String)value).replace("[","\\[");
////		                	value=((String)value).replace("/","\\/");
//                            value=((String)value).replace("\n","\\n");
//                            value=((String)value).replace("\r","\\r");
//                            value=((String)value).replace("\t","\\t");
//                            value=((String)value).replace("\"","\\\"");
//                            rowsBuilder.append("\""+value+"\"");
                            rowsBuilder.append("\""+encode((String)value,"UTF_16BE")+"\"");
                            break;
		                case "char":case "nchar":
		                	value=value.toString();
//		                	value=((String)value).replace("\\","\\\\");
//		                	value=((String)value).replace("\n","\\n");
//		                	value=((String)value).replace("\r","\\r");
//		                	value=((String)value).replace("\t","\\t");
//		                	value=((String)value).replace("\"","\\\"");
//		                	rowsBuilder.append("\""+value+"\"");
		                	rowsBuilder.append("\""+encode((String)value,"UTF_16BE")+"\"");
		                	break;
		                default:
		                	rowsBuilder.append(value.toString());
		                	break;
	                }
                }
            }
            rowsBuilder.append("]");
        } while (rs.next()==true) ;
        rowsBuilder.append("]");

        //System.out.println(sb.toString());
        if (fileWriter!=null) {
        	fileWriter.write(rowsBuilder.toString());
        	fileWriter.flush();
			fileWriter.close();
			sb.append(rowCount);
        }else {
        	sb.append(rowsBuilder);
        }
        sb.append(']');
        return sb.toString();
    }

	public static long setParams(PreparedStatement statement,Object data,Integer[] types,boolean isSelect) throws SQLException { //
//    	System.out.print(types.toString());
    	Object[] rows=(Object[])data;
    	Object[] row;//=(Object[])rows[0];

    	int rowCount=rows.length;
    	int fieldsCount=types.length;

    	int tempCount=0;
    	double tempDouble;
    	Class<?> tempType;
    	for (int r=0;r<rowCount;r++){
    		row=(Object[])rows[r];
    		if (row.length<fieldsCount) {
    			tempCount=row.length;
    		} else {
    			tempCount=fieldsCount;
    		}

    		for (int c=0;c<tempCount;c++){
//    			System.out.println(c);
//    			System.out.print("\t");
    			if (row[c]==null){
    				//System.out.println("setNull:"+(c+1)+types[c]);
    				statement.setObject(c+1, null,types[c],4); //statement.setNull(c+1,types[c]);
    			}else{
    				//statement.setBigDecimal(c+1, (BigDecimal) row[c]);
                    //statement.setDouble(c+1, (Double) row[c]);
                    if (types[c]==8) {
                        tempType=row[c].getClass();
                        //System.out.println(tempType);
                        if (tempType==java.lang.Integer.class){
                            tempDouble=Double.valueOf(row[c].toString());
                            //statement.setInt(c+1,(int) row[c]) ;//,types[c],4);
                        }else if (tempType==java.lang.String.class){
                            tempDouble=Double.valueOf(row[c].toString());
                        }else if (tempType==java.lang.Boolean.class){
                            tempDouble=((boolean)row[c]==true)?1:0;;
                        }else{
                            tempDouble=((Double) row[c]);
                            if (tempDouble<0) {
                                tempDouble=tempDouble - 0.000001;
                            }else{
                                tempDouble=tempDouble + 0.000001;
                            }
                            //System.out.print("double"+tempDouble);
                        }
                        statement.setObject(c+1, tempDouble,types[c],4);
                    }else{
                        statement.setObject(c+1, row[c],types[c],4); //值可以是字符串,types为BigDecimal, 定义最大保留4位小数
                    }
                }
    		}
    		for (int i=row.length+1;i<=fieldsCount;i++){
    			statement.setNull(i,types[i - 1]);
    		}
    		if (isSelect!=true) statement.addBatch(); //select时不能执行addBatch,否则jconn4会报错
//    		System.out.print("\r\n");
    	}

    	return 1;
    }

//	public static long setParamsByFastJson(PreparedStatement statement,String data,Integer[] types,boolean isSelect) throws SQLException { //
////    	System.out.print(types.toString());
//    	JSONArray rows=JSON.parseArray(data);
//    	JSONArray row;//=(Object[])rows[0];
//
//    	int rowCount=rows.size();
//    	int fieldsCount=types.length;
//
//    	int tempCount=0;
//    	double tempDouble;
//    	Class<?> tempType;
//    	for (int r=0;r<rowCount;r++){
//    		row=rows.getJSONArray(r);
//    		if (row.size()<fieldsCount) {
//    			tempCount=row.size();
//    		} else {
//    			tempCount=fieldsCount;
//    		}
//
//    		for (int c=0;c<tempCount;c++){
////    			System.out.println(c);
////    			System.out.print("\t");
//    			Object value=row.get(c);
//    			if (value==null){
//    				//System.out.println("setNull:"+(c+1)+types[c]);
//    				statement.setObject(c+1, null,types[c],4); //statement.setNull(c+1,types[c]);
//    			}else{
//    				//statement.setBigDecimal(c+1, (BigDecimal) row[c]);
//                    //statement.setDouble(c+1, (Double) row[c]);
//                    if (types[c]==8) {
//                        tempType=value.getClass();
//                        //System.out.println(tempType);
//                        if (tempType==java.lang.Integer.class){
//                            tempDouble=Double.valueOf(value.toString());
//                        }else if (tempType==java.lang.String.class){
//                            tempDouble=Double.valueOf(value.toString());
//                        }else if (tempType==java.lang.Boolean.class){
//                            tempDouble=((boolean) value==true)?1:0;
//                        }else{
//                            tempDouble=((BigDecimal) value).doubleValue();
//                            if (tempDouble<0) {
//                                tempDouble=tempDouble - 0.000001;
//                            }else{
//                                tempDouble=tempDouble + 0.000001;
//                            }
//                            //System.out.print("double"+tempDouble);
//                        }
//                        statement.setObject(c+1, tempDouble,types[c],4);
//                    }else{
//                        statement.setObject(c+1, value,types[c],4); //值可以是字符串,types为BigDecimal, 定义最大保留4位小数
//                    }
//                }
//    		}
//    		for (int i=row.size()+1;i<=fieldsCount;i++){
//    			statement.setNull(i,types[i - 1]);
//    		}
//    		if (isSelect!=true) statement.addBatch(); //select时不能执行addBatch,否则jconn4会报错
////    		System.out.print("\r\n");
//    	}
//
//    	return 1;
//    }

	public static long setParams(PreparedStatement statement,String data,String option) throws SQLException {

		JsonArray rows=Json.parse(data).asArray();
		//types,isSelect,numberPrecision
		JsonObject optionObject=Json.parse(option).asObject();

		JsonArray jsonTypes=optionObject.get("types").asArray();
		int[] types = new int[jsonTypes.size()];
		for (int i=0;i<jsonTypes.size();i++) {
			types[i]=jsonTypes.get(i).asInt();
		}
		boolean isSelect=optionObject.get("isSelect").asBoolean();
		boolean numberPrecision=optionObject.get("numberPrecision").asBoolean();

//    	Object[] rows=(Object[])data;
    	JsonArray row;//=(Object[])rows[0];

    	int rowCount=rows.size();
    	int fieldsCount=types.length;

    	int tempCount=0;
    	double tempDouble;
    	int tempInt;
    	String tempString;
//    	Class<?> tempType;
    	for (int r=0;r<rowCount;r++){
    		row=rows.get(r).asArray();
    		if (row.size()<fieldsCount) {
    			tempCount=row.size();
    		} else {
    			tempCount=fieldsCount;
    		}

    		for (int c=0;c<tempCount;c++){
//    			System.out.print(row.get(c).isNull());
    			JsonValue value=row.get(c);
    			if (value.isNull()){ //null
    				//System.out.println("setNull:"+(c+1)+types[c]);
    				statement.setObject(c+1, null,types[c],4); //statement.setNull(c+1,types[c]);
    			}else{
                    if (types[c]==8) { //Decimal
                        if (value.isString()){
                            tempDouble=Double.valueOf(value.asString());
                        }else if (value.isBoolean()){
                            tempDouble=(value.asBoolean()==true)?1:0;
                        }else{
                            tempDouble=(value.asDouble());
                            if (numberPrecision == false) {
                            	if (tempDouble<0) {
                                    tempDouble=tempDouble - 0.000001;
                                }else{
                                    tempDouble=tempDouble + 0.000001;
                                }
                            }
                        }
                        statement.setObject(c+1, tempDouble,types[c],4);
                    }else if (types[c]==4) { //Integer
                        if (value.isString()){
                        	tempInt=Integer.valueOf(value.asString());
                        }else if (value.isBoolean()){
                        	tempInt=(value.asBoolean()==true)?1:0;
                        }else{
                        	tempInt=(int) value.asDouble(); //避免double转integer报错
                        }
                        statement.setObject(c+1, tempInt,types[c],4);
                    } else{ //String,Time,Date,DateTime
                    	tempString=value.isString()?value.asString() :value.toString();
                        statement.setObject(c+1, tempString,types[c],4); //值可以是字符串,types为BigDecimal, 定义最大保留4位小数
                    }
                }
    		}
    		for (int i=row.size()+1;i<=fieldsCount;i++){
    			statement.setNull(i,types[i - 1]);
    		}
    		if (isSelect!=true) statement.addBatch(); //select时不能执行addBatch,否则jconn4会报错
//    		System.out.print("\r\n");
    	}

    	return 1;
	}

    public static int executeUpdate(PreparedStatement statement) throws SQLException { //
    	return statement.executeUpdate();
    }

    public static String rs2Excel(Connection conn, String filename, String path, String option) throws Exception {
    	String fullPath = path + File.separator + filename;
//     	System.out.println("rs2Excel:"+fullPath);
    	BigExcelWriter writer = null;
    	try {
    		List<Sheet> sheets = parseJsonParam(option, Sheet.class);
    		File file = new File(fullPath);

    		if (file.exists()) {
    			file.delete();
    		}

    		writer = ExcelUtil.getBigWriter(fullPath);

    		int sheetIndex = 0;
    		for (Sheet sheet : sheets) {
    			Statement querySt = conn.createStatement();
    			try {
    				List<String> sheetHead = sheet.getTitle();
        			String sheetName = sheet.getName();
        			String serialName = sheet.getSerial();
        			writer.setSheet(sheetIndex);
        			writer.renameSheet(sheetName);
        			sheetIndex++;

        			String querySQL = sheet.getSql();
        			ResultSet rs = querySt.executeQuery(querySQL);

        			ResultSetMetaData rsmd = rs.getMetaData();
        			int fieldsCount = rsmd.getColumnCount();

        			java.util.List<Object> rowData;
        			int rowIndex = 0;

        			if (sheetHead == null || (sheetHead.size() == 0)) {
        				List<String> columns = new ArrayList<String>();

        				for (int i=0; i<fieldsCount; i++) {
        					columns.add(rsmd.getColumnLabel(i + 1));
        				}

        				sheetHead = columns;

        			}
        			//标题序号名称
        			if (serialName != null) {
        				sheetHead.add(0, serialName);
        			}

        			writer.writeHeadRow(sheetHead);
        			int titleHeight = sheet.getTitleHeight();

        			if (titleHeight != 0) {
        				writer.setRowHeight(0, titleHeight);
        			}

        			for (int i=0; i<sheetHead.size(); i++) {
        				Cell cell = writer.getCell(i, 0);

        				if (cell != null) {
        					cell.getCellStyle().setWrapText(true);
        				}
        			}

        			while(rs.next()) {
        				rowData = new ArrayList<Object>();
        				if (serialName != null) {
        					rowData.add(rowIndex + 1);
        				}

        				for (int i=0; i<fieldsCount; i++) {
        					rowData.add(rs.getObject(i + 1));
        				}

        				writer.writeRow(rowData);
        				rowIndex++;
        				//写入2000行释放一下资源
        				if (rowIndex % 2000 == 0) {
        					System.out.println("已写入了" + rowIndex + "行");
        					System.gc();
        				}

        			}
    			} finally {
    				if (querySt != null) querySt.close();
    			}

    		}
    	} finally {
    		if (writer != null) writer.close();
		}

		return fullPath;
	}

	public static void createSqliteFile(Connection conn, String filePath, String options) throws Exception{
		Statement targetStatement = null;
		Statement sourceStatement = null;
		PreparedStatement targetStatement2 = null;
		Connection targetConn = null;
		JsonObject optionsObj = Json.parse(options).asObject();
		String dbms = optionsObj.getString("dbms","");
//		System.out.println(dbms);
		String tablesStr = optionsObj.getString("tables","");
//		System.out.println(tablesStr);
		try {
			List<SqliteTable> tables = parseJsonParam(tablesStr, SqliteTable.class);
			Class.forName("org.sqlite.JDBC");
			File file = new File(filePath);

			if (!file.exists()) {
				boolean createNewFileResult = file.createNewFile();
				if (!createNewFileResult) {
					throw new Exception("create new File fail,please check permission");
				}
			}

			targetConn = DriverManager.getConnection("jdbc:sqlite:" + filePath);
			targetConn.setAutoCommit(false);
			targetStatement = targetConn.createStatement();

//            System.out.println(dbms);
			if (dbms.equals("pgsql")) {
// 			    System.out.println("createStatement(1003,1007)");
				sourceStatement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.FETCH_FORWARD); //ResultSet.TYPE_FORWARD_ONLY, ResultSet.FETCH_FORWARD
				conn.setAutoCommit(false);
				sourceStatement.setFetchSize(1000);
			}else {
// 			    System.out.println("createStatement()");
				sourceStatement = conn.createStatement();
			}

			for (SqliteTable table : tables) {
				String targetTableName = table.getTargetTableName();
				List<String> targetFields = table.getTargetFields();
				String querySourceDataSql = table.getQuerySourceDataSql();
				String createTargetTableSql = table.getCreateTargetTableSql();

				if (querySourceDataSql == null || "".equals(querySourceDataSql)) {
					throw new Exception("querySourceDataSql can not be empty or null!");
				}

				if (targetTableName == null || "".equals(targetTableName)) {
					throw new Exception("targetTableName can not be empty or null!");
				}

				if (createTargetTableSql == null || "".equals(createTargetTableSql)) {
					throw new Exception("createTargetTableSql can not be empty or null!");
				}

				//create targetTable
				String[] sqls = createTargetTableSql.split(";");

				for (String sql : sqls) {
					targetStatement.addBatch(sql);
				}

				targetStatement.executeBatch();
				targetConn.commit();

				//query data and save
				StringBuilder sb1 = new StringBuilder("insert into" + " " + targetTableName + " " + "(");

				for (int i=0; i<targetFields.size(); i++) {
					if (i != targetFields.size()-1) {
						sb1.append(targetFields.get(i)).append(",");
					} else {
						sb1.append(targetFields.get(i)).append(")").append(" ").append("values (");
					}
				}

				for (int i=0; i<targetFields.size(); i++) {
					if (i != targetFields.size()-1) {
						sb1.append("?").append(",");
					} else {
						sb1.append("?").append(")");
					}
				}

				targetStatement2 = targetConn.prepareStatement(sb1.toString());
				ResultSet rs = sourceStatement.executeQuery(querySourceDataSql);

                table.setCommitFrequency(1000);
				int rowCount = 0;

				while (rs.next()) {
					rowCount++;
					for (int i=0; i<targetFields.size(); i++) {
						targetStatement2.setObject(i+1, rs.getObject(targetFields.get(i)));
					}
					targetStatement2.execute();

					if (rowCount % table.getCommitFrequency() == 0) {
					    System.out.println(rowCount);
						targetConn.commit();
//						System.gc();
					}
				}

				targetConn.commit();
			}
		} finally {
			if (targetStatement2 != null) {
				targetStatement2.close();
			}

			if (sourceStatement != null) {
				sourceStatement.close();
			}

			if (targetStatement != null) {
				targetStatement.close();
			}

			if (targetConn != null) {
				targetConn.close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> parseJsonParam(String jsonString, Class classz) throws Exception {
		JSONArray array = JSONArray.parseArray(jsonString);
		return array.toJavaList(classz);
	}
//
//	public static boolean importExcelData(String filePath, String option) throws Exception {
//		InputStream is = new FileInputStream(new File(filePath));
//		List<Table> tables = parseJsonParam(option, Table.class);
//		Workbook wk = StreamingReader.builder()
//	               	.rowCacheSize(100)    // number of rows to keep in memory (defaults to 10)
//	                .bufferSize(4096)     // buffer size to use when reading InputStream to file (defaults to 1024)
//	                .open(is);            // InputStream or File for XLSX file (required)
//
//		for (org.apache.poi.ss.usermodel.Sheet sheet : wk){
//            System.out.println(sheet.getSheetName());
//            int count = 0;//获取表头列数
//            List<String> header = new ArrayList<>();//获取表头
//
//            for (Row r : sheet) {
//                if (r.getRowNum() == 0){
//                    count = r.getLastCellNum();
//                    for (Cell c : r) {
//                        header.add(getCellValue(c));
//                    }
//                }else{
//                    break;
//                }
//            }
//
//            List<Map<String,String>> dataMap = new ArrayList<>();
//
//            for (Row r : sheet) {
//                if (r.getRowNum() > 0){
//                    Map<String,String> dataMap1 = new HashMap<>();
//
//                    for (int i = 0; i < header.size(); i++) {
//                        dataMap1.put(header.get(i),getCellValue(r.getCell(i)));
//                    }
//
//
//
//                    dataMap.add(dataMap1);
//                }
//
//            }
//
//            System.out.println(dataMap);
//            System.out.println(dataMap.size());
//        }
//
//		return false;
//	}
//
//	public static String getCellValue(Cell cell){
//        String cellValue = "";
//        if (cell == null) {
//            return "";
//        }
//        switch (cell.getCellTypeEnum()){
//            case NUMERIC: //数字
//                if (HSSFDateUtil.isCellDateFormatted(cell)){//日期
//                    SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
//                    Date d = cell.getDateCellValue();
//                    cellValue = sd.format(d);
//                }else{
//                    //转换格式
//                    DataFormatter dataFormatter = new DataFormatter();
//                    dataFormatter.addFormat("###########", null);
//                    cellValue = dataFormatter.formatCellValue(cell);
//                }
//                break;
//            case STRING: //字符串
//                cellValue = String.valueOf(cell.getStringCellValue());
//                break;
//            case BOOLEAN: //Boolean
//                cellValue = String.valueOf(cell.getBooleanCellValue());
//                break;
//            case FORMULA: //公式
//                cellValue = String.valueOf(cell.getCellFormula());
//                break;
//            case BLANK: //空值
//                cellValue = "";
//                break;
//            case ERROR: //故障
//                cellValue = "非法字符";
//                break;
//            default:
//                cellValue = "未知类型";
//                break;
//        }
//        return cellValue;
//    }
	
	//数据库连接对象、文件路径、数据分隔符、插入sql、文件编码、占位符列类型（Json数组）、行号位置（没有为-1）、设置行数为一批、提交模式（ONCE：只提交一次，WITHBATCH：处理一批，提交一次）
    public static int txt2Table(Connection conn,String filename,String delimiter,String sql,String charset,String placeholderTypes,int lineNoPlace,int batchNum,String commitModel) throws Exception{
    	 List<List<Object>> txtList = null;
    	 List<Object> lineStrList = null;
    	 File file = null;
    	 BufferedReader br = null;
    	 PreparedStatement ps = null;
    	 String lineStr = "";
	     Object[] objArr = null;
	     String[] columnTypes = null;
	     //记录行号
	     int rowid = 0;
 
    	 try {
    		 	file = new File(filename);
    		    br = new BufferedReader(
    						 new InputStreamReader(
    								 new FileInputStream(file), charset));
    		     ps = conn.prepareStatement(sql);
    		     ParameterMetaData pmd = ps.getParameterMetaData();
    		     txtList = new ArrayList<List<Object>>();
    		   //解析占位符列placeholderColumns
    		     if(!placeholderTypes.equals("")) {
    		    	 columnTypes = placeholderTypes.split(","); 
    		     }
    		     
    			 //不为null或""时，继续读
    			 while ((lineStr = br.readLine()) != null) {
    				 if(lineStr.trim().isEmpty()) break;
    				 rowid++;
    				 //依据分隔符获取具体数据
    				 objArr = lineStr.split(delimiter);
    				 //防止获取元素下标超界
    				 objArr = Arrays.copyOf(objArr, pmd.getParameterCount());
    				 lineStrList = new ArrayList<Object>(Arrays.asList(objArr));
    				 if(lineNoPlace != -1) {
    					 //如果有行号，在行号位置设置行号
    					 lineStrList.add(lineNoPlace, rowid); 
    				 }
    				 txtList.add(lineStrList);
    		         //每读batchNum行插入一次数据
    		         if(rowid % batchNum == 0) {
    		        	 //调用方法插入数据,然后清空list
    		             insertTable(ps, txtList, columnTypes, pmd, lineNoPlace);
    		             txtList.clear();
    		             if(commitModel.equals("WITHBATCH")) {
    	    		    	 conn.commit(); 
    	    		     }
    		         }
    			 }
    			 
    		     if(txtList.size()>0) {
    		    	 //循环完成之后，提交最后一批不足batchNum条的数据
    		    	 insertTable(ps, txtList, columnTypes, pmd, lineNoPlace);
    		    	 if(commitModel.equals("WITHBATCH")) {
        		    	 conn.commit(); 
        		     }
    		     }
    		     if(commitModel.equals("ONCE")) {
    		    	 conn.commit(); 
    		     }
		} catch (Exception e) {
			throw e;
		}finally {
			if(ps != null) {
				 ps.close();
			 }
			//先关闭流，再删除文件
			if(br != null) {
				 br.close();
			 }
			 file.delete();
		}
    	 return rowid;
    }
    //PreparedStatement对象、待插入的数据、占位符列类型、ParameterMetaData对象（用于查询占位符个数）、行号位置
    public static void insertTable(PreparedStatement ps,List<List<Object>> list,String[] placeholderTypes,ParameterMetaData pmd,int lineNoPlace) throws Exception{
    	int targetSqlType = 12;
    	//循环遍历list，每次遍历取出的数据为txt文件一行的记录
        for(List<Object> objArr : list){
        	for(int i = 0; i < pmd.getParameterCount(); i++) {
        		if(placeholderTypes!=null && i < placeholderTypes.length ) {
        			targetSqlType = Integer.valueOf(placeholderTypes[i]);
        		}
        		if(lineNoPlace == i) {
        			ps.setObject(i+1,  objArr.get(i),  4, 4);
        		}else {
        			ps.setObject(i+1,  objArr.get(i),  targetSqlType, 4);
				}
        	}
			ps.addBatch();
        }
		ps.executeBatch();
    }
}

