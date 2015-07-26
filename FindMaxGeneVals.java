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

public class FindMaxGeneVals extends EvalFunc<String>
{
	public static double toDouble(byte[] bytes) throws IOException{
      LoadCaster lc = new Utf8StorageConverter();
      return  lc.bytesToDouble(bytes);
   }
	public String exec(Tuple input) throws IOException{
		try{
			DataByteArray s1 = (DataByteArray) input.get(0);
			String sampleName = s1.toString();
			int numberOfGenes = input.size();
			double max = 0.0;
			String results = sampleName;
			for(int i = 1; i<numberOfGenes; i++){
				DataByteArray v1 = (DataByteArray) input.get(i);
				double val1 = toDouble(v1.get());
				if(val1 >= max){
					max = val1;
				}
			}
			for(int j = 1; j<numberOfGenes; j++){
				DataByteArray v2 = (DataByteArray) input.get(j);
				double val2 = toDouble(v2.get());
				if(val2 >= max){
					results+=",gene_"+Integer.toString(j);
				}
			}
			return results;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row "+e.getMessage(),e);
		}
	}
}