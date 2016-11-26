package process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.HiveParser.fileFormat_return;
import org.apache.hadoop.hive.ql.parse.HiveParser.queryStatementExpression_return;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;

import model.Event;
import scala.Array;

public class Process {
	static Connection connection;

	private static Connection createConnection() throws Exception {
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
	}

	public static void exportHourlyUser(SparkSession spark) {
		long currentDate = System.currentTimeMillis() / 1000;
		long hourBefore = System.currentTimeMillis() / 1000 - 72 * 3600;
		String query = "SELECT DISTINCT uuid FROM events WHERE created_at >= " + hourBefore + " AND created_at <= "
				+ currentDate;
		System.out.println(query);
		Dataset<Row> uuids = spark.sql(query);

		JavaRDD<Row> rdd = uuids.javaRDD();
		JavaRDD<String> rddStr = rdd.map(new Function<Row, String>() {

			@Override
			public String call(Row row) throws Exception {
				System.out.println("'" + (String)row.get(0) + "'");
				return (String) row.get(0);
			}
		});

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String date = sdf.format(new Date(currentDate * 1000));
		Iterator<String> iterator = rddStr.toLocalIterator();
		StringBuilder sb = new StringBuilder();
		while (iterator.hasNext()) {
			String n = (String) iterator.next();
			if (sb.length() > 0)
				sb.append(',');
			sb.append("('").append(n).append("','").append(date).append("')");
			
		}
//		List<String> listVal = rddStr.collect();
//
//		for (int i = 0; i < listVal.size(); i++) {
//			listVal.set(i, "('" + listVal.get(i) + "','" + date + "')");
//		}
//
//		StringBuilder sb = new StringBuilder();
//		for (String n : listVal) {
//			if (sb.length() > 0)
//				sb.append(',');
//			sb.append("('").append(n).append("','").append(date).append("')");
//		}
		String values = sb.toString();

		// String values = rddStr.reduce(new Function2<String, String, String>()
		// {
		//
		// @Override
		// public String call(String v1, String v2) throws Exception {
		// System.out.println("v1: '" + v1 + "'");
		// System.out.println("v2: '" + v2 + "'");
		// String res = v1 + "('" + v2 + "','" + date + "'),";
		// System.out.println("res: " + res);
		// return res;
		// }
		// });
		// String values = rddStr.fold("|", new Function2<String, String,
		// String>() {
		//
		// @Override
		// public String call(String v1, String v2) throws Exception {
		// if (v2.trim().equals("") || v2.equals("|")) {
		// return v1;
		// }
		// System.out.println("v1: '" + v1 + "'");
		// System.out.println("v2: '" + v2 + "'");
		// String res = v1 + "('" + v2 + "','" + date + "'),";
		// System.out.println("res: " + res);
		// return res;
		// }
		// });
		// values = values.substring(0, values.length() - 2);
		try {
			connection.createStatement();
			String q = String.format("INSERT INTO user_actives(uuid, created_at) VALUES %s", values);
			System.out.println(q);
			PreparedStatement stm = connection.prepareStatement(q);
			stm.execute();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			connection = createConnection();
			connection.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		SparkSession spark = SparkSession.builder().appName("ProcessingData")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate();
		spark.sql("show tables").show();

		exportHourlyUser(spark);

	}
}
