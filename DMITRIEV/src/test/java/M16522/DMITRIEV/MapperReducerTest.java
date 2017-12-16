package M16522.DMITRIEV;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import M16522.DMITRIEV.HBPICount.Map;
import M16522.DMITRIEV.HBPICount.Reduce;



public class MapperReducerTest {
	MapDriver<Object, Text, IntWritable, IntWritable> mapDriver;
	ReduceDriver<IntWritable, IntWritable, Text, IntWritable> reduceDriver;
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

	@Before
	public void setUp() {
		Map mapper = new Map();
		Reduce reducer = new Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	/**
	 * Tests map with correct input.
	 */
	@Test
	public void testMapperCorrectInput() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("3181e2272b4ce146851105f14bc8b5ae"
				+ "	20131023174902964	1	CAPNBf7jdso	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36"
				+ " (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36	111.164.198.*	2	2	3	"
				+ "9ae7272f35d7c6b5f7ccc8702bb2115f	9457e88b9a7945d91380678b0b86921e	null	discuz_2908326_001"
				+ "	960	90	Na	Na	5	10717	294	5	null	2821	11278,14273,11576,11632,13042,10083,11423,1"
				+ "0110,13776,13403,10115,10133,10063,11092,10057,11724,13800,10684,10076,10077,10093,10024,13866,1000"
				+ "6,16706,10031,10146,10052,10125"));
		mapDriver.withOutput(new IntWritable(2),
				new IntWritable(1));
		mapDriver.runTest();
	}

	/**
	 * Tests map with incorrect length of line.
	 */
	@Test
	public void testMapperBadLengthOfLine() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("3181e2272b4ce146851105f14bc8b5ae"
				+ "	20131023174902964	CAPNBf7jdso	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36"
				+ " (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36	111.164.198.*	2	2	3	"
				+ "9ae7272f35d7c6b5f7ccc8702bb2115f	9457e88b9a7945d91380678b0b86921e	null	discuz_2908326_001"
				+ "	960	90	Na	Na	5	10717	294	5	null	2821	11278,14273,11576,11632,13042,10083,11423,1"
				+ "0110,13776,13403,10115,10133,10063,11092,10057,11724,13800,10684,10076,10077,10093,10024,13866,1000"
				+ "6,16706,10031,10146,10052,10125"));
		mapDriver.runTest();
	}

	/**
	 * Tests map with low bidding price.
	 */
	@Test
	public void testMapperLowBidPrice() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("3181e2272b4ce146851105f14bc8b5ae"
				+ "	20131023174902964	1	CAPNBf7jdso	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36"
				+ " (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36	111.164.198.*	2	2	3	"
				+ "9ae7272f35d7c6b5f7ccc8702bb2115f	9457e88b9a7945d91380678b0b86921e	null	discuz_2908326_001"
				+ "	960	90	Na	Na	5	10717	234	5	null	2821	11278,14273,11576,11632,13042,10083,11423,1"
				+ "0110,13776,13403,10115,10133,10063,11092,10057,11724,13800,10684,10076,10077,10093,10024,13866,1000"
				+ "6,16706,10031,10146,10052,10125"));
		mapDriver.runTest();
	}

	/**
	 * Tests map with bad id.
	 */
	@Test
	public void testMapperBadId() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("3181e2272b4ce146851105f14bc8b5ae"
				+ "	20131023174902964	1	CAPNBf7jdso	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36"
				+ " (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36	111.164.198.*	2	as2	3	"
				+ "9ae7272f35d7c6b5f7ccc8702bb2115f	9457e88b9a7945d91380678b0b86921e	null	discuz_2908326_001"
				+ "	960	90	Na	Na	5	10717	294	5	null	2821	11278,14273,11576,11632,13042,10083,11423,1"
				+ "0110,13776,13403,10115,10133,10063,11092,10057,11724,13800,10684,10076,10077,10093,10024,13866,1000"
				+ "6,16706,10031,10146,10052,10125"));
		mapDriver.runTest();
	}

	@Before
	public void setUpStreams() {
		System.setOut(new PrintStream(outContent));
	}

	/**
	 * Tests finding cache files error in reducer.
	 */
	@Test
	public void testReducerCacheFileNotFound() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new IntWritable(3), values);
		reduceDriver.runTest();
		assertEquals("Cached file is not found\n\r", outContent.toString());
	}

	@After
	public void cleanUpStreams() {
		System.setOut(null);
		System.setErr(null);
	}

	/**
	 * Tests reduce with correct output.
	 */
	@Test
	public void testReducerCorrectOutput() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new IntWritable(4), values);
		reduceDriver.withCacheFile("/root/hw1/city.en.txt");
		reduceDriver.withCacheFile("/root/hw1/region.en.txt");
		reduceDriver.withOutput(new Text("shijiazhuang"), new IntWritable(3));
		reduceDriver.runTest();
	}

	/**
	 * Tests reduce with unknown city.
	 */
	@Test
	public void testReducerUnknownCity() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new IntWritable(550), values);
		reduceDriver.withCacheFile("/root/hw1/city.en.txt");
		reduceDriver.withCacheFile("/root/hw1/region.en.txt");
		reduceDriver.withOutput(new Text("UNKNOWN"), new IntWritable(3));
		reduceDriver.runTest();
	}

}
