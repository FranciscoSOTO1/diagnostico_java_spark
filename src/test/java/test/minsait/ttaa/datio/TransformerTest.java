package test.minsait.ttaa.datio;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import minsait.ttaa.common.pathfile.PathFile;
import minsait.ttaa.datio.engine.Transformer;


public class TransformerTest implements SparkSessionTestWrapper {

	@Test
	public void testTransformer() {

		PathFile path= new PathFile();
        Transformer engine = new Transformer(spark,path);
      
		try {
			assertTrue(true);
		} catch (Exception e) {
			assertTrue(false);	
		}
	}

}
