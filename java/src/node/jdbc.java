package node;

//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
//import java.io.OutputStream;
//import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
//import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.BitSet;

//import netscape.javascript.JSObject;

public class jdbc {

	public static void main(String[] args) throws SQLException, UnsupportedEncodingException {
		
		System.out.println(encode("\"\\12	3夏子aa夏ABC&://-+ 1\n\r23","UTF_16BE")); 
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
    
    public static String rs2table3(ResultSet rs) throws SQLException, IOException { //
//    	System.out.println("rs2table3");
        ResultSetMetaData rsmd = rs.getMetaData();//
        int fieldsCount=rsmd.getColumnCount();

        Object value="";
        
        int rowCount=0;
		StringBuilder sb=new StringBuilder();
		
		if (rs.next()==true) {
			sb.append("[");
		} else {
			return null;
		}
        do{
        	rowCount++;
        	if (rowCount==1){
        		sb.append("[");	
        	}else{
        		sb.append(",[");
        	}
        	
            for (int i=0;i<fieldsCount;i++){
            	if (i!=0) sb.append(",");
                value=rs.getObject(i + 1);
                if (value==null){
                	sb.append("null");
                }else{
//                	System.out.print(rsmd.getColumnTypeName(i+1)+',');
	                switch (rsmd.getColumnTypeName(i+1).toLowerCase()){
	                	case "varbinary":
	                		sb.append("0");
		                    break;
		                case "datetime":case "date":case "time":case "timestamp":
		                	sb.append("\""+value.toString()+"\"");
		                    break;
		                case "varchar":case "text":case "nvarchar":
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
//                            sb.append("\""+value+"\"");
                            sb.append("\""+encode((String)value,"UTF_16BE")+"\"");
                            break;
		                case "char":case "nchar":
		                	value=value.toString();
//		                	value=((String)value).replace("\\","\\\\");
//		                	value=((String)value).replace("\n","\\n");
//		                	value=((String)value).replace("\r","\\r");
//		                	value=((String)value).replace("\t","\\t");
//		                	value=((String)value).replace("\"","\\\"");
//		                	sb.append("\""+value+"\"");
		                	sb.append("\""+encode((String)value,"UTF_16BE")+"\"");
		                	break;
		                default:
		                	sb.append(value.toString());
		                	break;
	                }
                }
            }
            sb.append("]");
        } while (rs.next()==true) ;
        sb.append("]");
        //System.out.println(sb.toString());
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

    public static int executeUpdate(PreparedStatement statement) throws SQLException { //
    	return statement.executeUpdate();
    }
    

}

