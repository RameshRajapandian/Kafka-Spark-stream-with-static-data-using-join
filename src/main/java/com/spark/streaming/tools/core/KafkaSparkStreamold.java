package com.spark.streaming.tools.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSparkStreamold {
	static int id;

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream")
				.setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");

		SQLContext spark = new SQLContext(sc);
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "root");
		connectionProperties.put("password", "password");
		String query = "SELECT * FROM contact";

		DataFrame jdbcDF2 = spark
				.read()
				.jdbc("jdbc:mysql://localhost:3306/test", "contact",
						connectionProperties).cache();

		jdbcDF2.registerTempTable("contactTable");

		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(
				5000));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		Set<String> topicName = Collections.singleton("test");

		JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicName);

		JavaDStream<String> msgStream = kafkaSparkPairInputDStream
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2)
							throws Exception {
						return tuple2._2();
					}
				});

		JavaDStream<Contact> jsonInputDTORDD = msgStream
				.map(new Function<String, Contact>() {
					@Override
					public Contact call(String string) throws Exception {
						return new ObjectMapper().readValue(string,
								Contact.class);
					}
				});

		JavaPairDStream<Integer, Contact> jsonInputDTORDD1 = jsonInputDTORDD
				.mapToPair(read -> {
					id = read.getContactId();
					return new Tuple2<Integer, Contact>(read.getContactId(),
							read);
				});

		JavaPairDStream<Integer, Contact> jsonPairRdd = jsonInputDTORDD1
				.transformToPair(new Function<JavaPairRDD<Integer, Contact>, JavaPairRDD<Integer, Contact>>() {
					@Override
					public JavaPairRDD<Integer, Contact> call(
							JavaPairRDD<Integer, Contact> contact)
							throws Exception {
						return contact;
					}
				});

		DataFrame df;
		df = spark
				.sql("select contactId, sanlimt from contactTable where contactId = "
						+ id);

		JavaRDD<Row> rowRdd = df.toJavaRDD();
		JavaRDD<Contact> rowRDD = rowRdd.map(new Function<Row, Contact>() {
			@Override
			public Contact call(Row r) throws Exception {
				Contact c = new Contact();
				c.setContactId(r.getInt(0));
				c.setSanlimt(r.getAs("sanlimt"));
				System.out.println("sanlimt" + c.getSanlimt());
				return c;
			}
		});

		JavaPairRDD<Integer, Contact> pairRdd = rowRDD
				.mapToPair(new PairFunction<Contact, Integer, Contact>() {
					@Override
					public Tuple2<Integer, Contact> call(Contact c)
							throws Exception {
						return new Tuple2<Integer, Contact>(c.getContactId(), c);
					}
				});

		JavaPairDStream<Integer, Tuple2<Contact, Contact>> joinedDS = jsonPairRdd
				.transformToPair(rdd -> rdd.join(pairRdd));

		JavaDStream<Contact> updateContact = joinedDS
				.map(new Function<Tuple2<Integer, Tuple2<Contact, Contact>>, Contact>() {
					@Override
					public Contact call(
							Tuple2<Integer, Tuple2<Contact, Contact>> mergerContact)
							throws Exception {
						Tuple2<Contact, Contact> tempContact = mergerContact
								._2();
						tempContact._1().setSanlimt(
								tempContact._2().getSanlimt());
						System.out.println("Final o/p ="
								+ tempContact._1().toString());
						return tempContact._1();
					}
				});

		updateContact.print();

		// }

		ssc.start();
		ssc.awaitTermination();
	}
}