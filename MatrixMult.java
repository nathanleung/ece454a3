import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.hadoop.io.WritableComparable;
import java.nio.ByteBuffer;
import org.apache.pig.LoadCaster;
import org.apache.pig.builtin.Utf8StorageConverter;
import java.util.*;

public class MatrixMult extends EvalFunc<String>
{
	public static double toDouble(byte[] bytes) throws IOException{
      LoadCaster lc = new Utf8StorageConverter();
      return  lc.bytesToDouble(bytes);
   }
	public String exec(Tuple input) throws IOException{
		try{
			DataByteArray s1 = (DataByteArray) input.get(0);
			String sampleName1 = s1.toString();
			int sNum1 = Integer.parseInt(sampleName1.split("_")[1]);
			int doubleNumberOfGenes = input.size();
			DataByteArray s2 = (DataByteArray) input.get(doubleNumberOfGenes/2);
			String sampleName2 = s2.toString();
			int sNum2 = Integer.parseInt(sampleName2.split("_")[1]);
			if( sNum1 >= sNum2){
				return "";
			}
			String results = sampleName1+","+sampleName2+",";
			double product = 0.0;
			double sum = 0.0;
			for(int i = 1; i<doubleNumberOfGenes/2; i++){
				DataByteArray v1 = (DataByteArray) input.get(i);
				double val1 = toDouble(v1.get());
				for(int j = doubleNumberOfGenes/2 +1; j< doubleNumberOfGenes; j++ )
				{
					DataByteArray v2 = (DataByteArray) input.get(j);
					double val2 = toDouble(v2.get());
					product = val1*val2;
					sum+=product;
				}
			}
			results+=Double.toString(sum);
			return results;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row "+e.getMessage(),e);
		}
	}
}