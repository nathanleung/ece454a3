import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import java.util.Iterator;

public class part2pig extends EvalFunc<DataBag>
{
	public DataBag exec(Tuple input) throws IOException {
		try {
			int genesNum = 0;
			double genesEntry = 0.0;
			BagFactory bf = BagFactory.getInstance(); 
			TupleFactory tf = TupleFactory.getInstance();
			DataBag genes = (DataBag)input.get(0);
			DataBag res = bf.newDefaultBag();
			Iterator i = genes.iterator();
			int tupleSize = input.size();
			while (i.hasNext()) {
				genesNum++;
				genesEntry = Double.parseDouble(((Tuple)i.next()).get(0).toString());
				Tuple t = tf.newTuple();
				t.append("gene_" + genesNum);
				if (genesEntry > 0.5)
					t.append(1);
				else if (genesEntry <= 0.5)
					t.append(0);
				res.add(t);
			}
			return res;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " +e.getMessage(), e);
		}
	}
} 
