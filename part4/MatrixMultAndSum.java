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

public class MatrixMultAndSum extends EvalFunc<String>
{
	public static double toDouble(byte[] bytes) throws IOException{
      LoadCaster lc = new Utf8StorageConverter();
      return  lc.bytesToDouble(bytes);
   }
	public String exec(Tuple input) throws IOException{
		try{
			Tuple s1 = (Tuple) input.get(0);
			String s1Name = (String) s1.get(0) +"_"+ (String)s1.get(1);
			DataBag genes1 = (DataBag) input.get(1);
			Tuple s2 = (Tuple) input.get(2);
			String s2Name = (String) s2.get(0) +"_"+ (String)s2.get(1);
			DataBag genes2 = (DataBag) input.get(3);
			Iterator i1 = genes1.iterator();
			Iterator i2 = genes2.iterator();
			Double product = 0.0;
			Double sum = 0.0;
			String result = s1Name+","+s2Name+",";
			while(i1.hasNext()){
				Tuple gene1 = (Tuple)i1.next();
				double gene1Val = toDouble(((DataByteArray)gene1.get(0)).get());
				Tuple gene2 = (Tuple)i2.next();
				double gene2Val = toDouble(((DataByteArray)gene2.get(0)).get());
				if(gene1Val > 0.0 && gene2Val > 0.0){
					product = gene1Val*gene2Val;
					sum+=product;
				}
			}
			result+=Double.toString(sum);
			return result;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row "+e.getMessage(),e);
		}
	}
}