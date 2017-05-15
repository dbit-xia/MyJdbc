package node;

//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
import java.io.IOException;
//import java.io.OutputStream;
//import java.io.StringWriter;
import java.math.BigDecimal;
//import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;

//import netscape.javascript.JSObject;

public class jdbc {

	public static void main(String[] args) throws SQLException {
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
                            value=((String)value).replace("\\","\\\\");
//		                	value=((String)value).replace("{","\\{");
//		                	value=((String)value).replace("[","\\[");
//		                	value=((String)value).replace("}","\\}");
//		                	value=((String)value).replace("[","\\[");
//		                	value=((String)value).replace("/","\\/");
                            value=((String)value).replace("\n","\\n");
                            value=((String)value).replace("\r","\\r");
                            value=((String)value).replace("\t","\\t");
                            value=((String)value).replace("\"","\\\"");
                            sb.append("\""+value+"\"");
                            break;
		                case "char":case "nchar":
		                	value=value.toString();
		                	value=((String)value).replace("\\","\\\\");
		                	value=((String)value).replace("\n","\\n");
		                	value=((String)value).replace("\r","\\r");
		                	value=((String)value).replace("\t","\\t");
		                	value=((String)value).replace("\"","\\\"");
		                	sb.append("\""+value+"\"");
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
    	for (int r=0;r<rowCount;r++){
    		row=(Object[])rows[r];
    		if (row.length<fieldsCount) {
    			tempCount=row.length;
    		} else {
    			tempCount=fieldsCount;
    		}
    		
    		for (int c=0;c<tempCount;c++){
//    			System.out.print(row[c]);
//    			System.out.print("\t");
    			if (row[c]==null){
    				statement.setNull(c+1,types[c]);
    			}else{
    				//statement.setBigDecimal(c+1, (BigDecimal) row[c]);
//    				statement.setDouble(c+1, (Double) row[c]);
    				statement.setObject(c+1, row[c],types[c],4); //值可以是字符串,types为BigDecimal, 定义最大保留4位小数
    			}
    		}
    		for (int i=row.length+1;i<=fieldsCount;i++){
    			statement.setNull(i,types[i - 1]);
    		}
    		if (isSelect!=true) statement.addBatch(); //select时不能执行addbatch,否则jconn4会报错
//    		System.out.print("\r\n");
    	}
    	
    	return 1;
    }

    public static int executeUpdate(PreparedStatement statement) throws SQLException { //
    	return statement.executeUpdate();
    }
}
