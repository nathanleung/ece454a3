import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.hadoop.io.WritableComparable;
import java.nio.ByteBuffer;
import org.apache.pig.LoadCaster;
import org.apache.pig.builtin.Utf8StorageConverter;
import java.util.*;

public class MultPair extends EvalFunc<Tuple>
{
	public static double toDouble(byte[] bytes) throws IOException{
      LoadCaster lc = new Utf8StorageConverter();
      return  lc.bytesToDouble(bytes);
   }
	public Tuple exec(Tuple input) throws IOException{
		try{
			Tuple s1 = (Tuple) input.get(1);
			String s1Name = (String) s1.get(0);
			String s1Val = (String) s1.get(1);
			int s1Num = Integer.parseInt(s1Name.split("_")[1]);

			Tuple s2 = (Tuple) input.get(3);
			String s2Name = (String) s2.get(0);
			String s2Val = (String) s2.get(1);
			TupleFactory tf = TupleFactory.getInstance();
			Tuple t1 = tf.newTuple();
			int s2Num = Integer.parseInt(s2Name.split("_")[1]);
			if(s1Num >= s2Num){
				return t1;
			}

			String key = s1Name+","+s2Name;
			Double s1exprVal = Double.parseDouble(s1Val);
			Double s2exprVal = Double.parseDouble(s2Val);
			Double product = s1exprVal*s2exprVal;
			t1.append(key);
			t1.append(product);
			return t1;
			
		}catch(Exception e){
			throw new IOException("Caught exception processing input row "+e.getMessage(),e);
		}
	}
}