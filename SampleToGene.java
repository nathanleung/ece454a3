import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.hadoop.io.WritableComparable;
import java.nio.ByteBuffer;
import org.apache.pig.LoadCaster;
import org.apache.pig.builtin.Utf8StorageConverter;
import java.util.*;

public class SampleToGene extends EvalFunc<DataBag>
{
	public static double toDouble(byte[] bytes) throws IOException{
      LoadCaster lc = new Utf8StorageConverter();
      return  lc.bytesToDouble(bytes);
   }
	public DataBag exec(Tuple input) throws IOException{
		try{
			DataByteArray s1 = (DataByteArray) input.get(0);
			String sampleName = s1.toString();
			int numberOfGenes = input.size();
			double max = 0.0;
			BagFactory bf = BagFactory.getInstance();
			DataBag results = bf.newDistinctBag();
			String geneName = "gene_";
			TupleFactory tf = TupleFactory.getInstance();
			for(int i = 1; i<numberOfGenes; i++){
				DataByteArray v1 = (DataByteArray) input.get(i);
				double exprVal = toDouble(v1.get());
				Tuple geneTuple = tf.newTuple();
				if(exprVal > 0.0){
					String geneNum = geneName+Integer.toString(i);
					geneTuple.append(geneNum);
					Tuple sampleTuple = tf.newTuple();
					sampleTuple.append(sampleName);
					sampleTuple.append(Double.toString(exprVal));
					geneTuple.append(sampleTuple);
					results.add(geneTuple);
				}
			}
			return results;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row "+e.getMessage(),e);
		}
	}
}