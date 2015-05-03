package com.my.hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @author taox
 */
public class MostValueUDAF extends AbstractGenericUDAFResolver {

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		return new MostValueUDAFEvaluator();
	}
	
	public static class MostValueUDAFEvaluator extends GenericUDAFEvaluator {

		@Override
	    public ObjectInspector init(Mode m, ObjectInspector[] parameters){
			return null;
		}
		
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			return null;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			return null;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			return null;
		}
		
		
		
	}
}
