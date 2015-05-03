package com.my.hive.udtf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 把uuid_play表按照 stime, etime 切分成多行
 * 如：20150501 08:10:01 20150501 13:30:01 
 *    将输出    8
 *          10
 *          12
 * @author taox
 */
public class SumUuidPlay2MutiByTime extends GenericUDTF {

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		if (argOIs.length != 2) {
            throw new UDFArgumentLengthException("SumUuidPlay2MutiByTime need two argument");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("SumUuidPlay2MutiByTime takes string as a parameter");
        }
        if (argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("SumUuidPlay2MutiByTime takes string as a parameter");
        }
        
		//20150501 08:10:01 
		//20150501 11:30:01
		ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("interval2time");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector
				(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		//20150501 08:10:01 20150501 11:30:01
		String stime = args[0].toString();
		String etime = args[1].toString();
		Calendar cal = Calendar.getInstance();
		try {
			Date sdate = DateUtils.parseDate(stime, new String[]{"yyyyMMdd HH:mm:ss"});
			Date edate = DateUtils.parseDate(etime, new String[]{"yyyyMMdd HH:mm:ss"});
			
			Date sdayOfDate = DateUtils.truncate(sdate, Calendar.DAY_OF_MONTH);
			Date edayOfDate = DateUtils.truncate(edate, Calendar.DAY_OF_MONTH);
			//假如时间跨天，返回stime的时间
			if(edayOfDate.getTime()>sdayOfDate.getTime()){
				cal.setTime(sdate);
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				if(hour > 0 ){
					hour = hour - (hour % 2 );
				}
				forward(new String[]{hour+""});
			}else{
				//时间不跨天
				cal.setTime(sdate);
				int shour = cal.get(Calendar.HOUR_OF_DAY);
				cal.setTime(edate);
				int ehour = cal.get(Calendar.HOUR_OF_DAY);
				
				shour = shour - (shour % 2 );
				ehour = ehour - (ehour % 2 );
				
				if(shour>ehour){
					int chg = shour;
					shour = ehour;
					ehour = chg;
				}
				for(int i=shour;i<=ehour;i+=2){
					forward(new String[]{i+""});
				}
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			throw new HiveException(e);
		}
	}

	@Override
	public void close() throws HiveException {

	}
	
	public void forward2(Object obj){
		System.out.println(obj);
	}
	
	public static void main(String[] args) throws HiveException {
//		SumUuidPlay2MutiByTime s = new SumUuidPlay2MutiByTime();
//		s.process(new String[]{"20150501 05:24:16","20150501 00:21:26"});
	}

}
