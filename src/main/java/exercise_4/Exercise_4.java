package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.stream.Collectors;
import java.io.IOException;


import java.util.List;

public class Exercise_4 {

	private static Row createRow(String wikiRow){
		String[] p = wikiRow.split("\t");
		return RowFactory.create(p[0], p[1]);
	}

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		Path wikiEP = Paths.get("/home/himanshu/SDM_lab2/SparkGraphXassignment/src/main/resources/wiki-edges.txt");
		Path wikiVP = Paths.get("/home/himanshu/SDM_lab2/SparkGraphXassignment/src/main/resources/wiki-vertices.txt");

		try{
			List<Row> vertices_list = Files.lines(wikiVP).map(s -> createRow(s)).collect(Collectors.toList());
			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
			StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
			});
			Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);


			List<Row> edges_list = Files.lines(wikiEP).map(s -> createRow(s)).collect(Collectors.toList());
			JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
			StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
			});
			Dataset<Row> edges =  sqlCtx.createDataFrame(edges_rdd, edges_schema);

			GraphFrame gf = GraphFrame.apply(vertices,edges);

			List<Double> prob = List.of(0.1, 0.3, 0.5);
			List<Integer> max_it = List.of(5, 10, 15);

			for (Double i : prob) {
				for (Integer j : max_it) {
					long start = System.currentTimeMillis();
					
					GraphFrame pagerank_graph = gf.pageRank().resetProbability(i).maxIter(j).run();
					pagerank_graph.vertices().orderBy(org.apache.spark.sql.functions.desc("pagerank")).limit(10).show();
	
					long end = System.currentTimeMillis();
	
					System.out.println("\nDamping Factor: " + (1 - i));
					System.out.println("Max Iterations: " + j);
					System.out.println("Time(ms): " + (end - start));
				}
	
			}
		
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}
	
}