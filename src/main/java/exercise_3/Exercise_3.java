package exercise_3;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Collections;

public class Exercise_3 {

    //scala.Function3<Object,VD,A,VD> vprog
    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Long>> apply(Long vertexID, Tuple2<Integer,List<Long>> vertexValue, Tuple2<Integer,List<Long>> message) {
            // Just keep the minimum of current value and pathsum (distance till adjecent source vertex + edge weight of previous adjecent vertex to current vertex)
            if (message._1()<vertexValue._1()){
                return message;
            }
            else{
                return vertexValue;
            }
        }
    }

    //scala.Function1<EdgeTriplet<VD,ED>,scala.collection.Iterator<scala.Tuple2<Object,A>>> sendMsg
    // changed the type according to our defined type for message and vertex as Tuple2<Integer,List<Long>>
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Long>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer,List<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer,List<Long>>>> apply(EdgeTriplet<Tuple2<Integer,List<Long>>, Integer> triplet) {
            // get the sourceVertex Tuple containing a. Object(vertex ID), b. cost value, c. traversed path
            Tuple2<Object,Tuple2<Integer,List<Long>>> sourceVertex = triplet.toTuple()._1();
            // get the destination vertex Tuple containing a. Object(vertex ID), b. cost value, c. traversed path
            Tuple2<Object,Tuple2<Integer,List<Long>>> dstVertex = triplet.toTuple()._2();
            // get the weight of between path
            int edge_weight = triplet.attr();
            
            // save the cost and traversed path in new variables for both sources and destination vertex
            Tuple2<Integer,List<Long>> sourceVertex_val = sourceVertex._2();
            Tuple2<Integer,List<Long>> dstVertex_val = dstVertex._2();

            // if source vertex is infinite(initial vertex values) or destination vertex is smaller then the new calculated path, do nothing
            if (sourceVertex_val._1 ==Integer.MAX_VALUE ||  (dstVertex_val._1 <= sourceVertex_val._1 + edge_weight )){
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Long>>>>().iterator()).asScala();
            }
            // else send message as pathsum (distance till source vertex + edge weight to adjecent vertex) and updated traversed list to destination vertex
            else{
                // cost of path till the destination vertex
                int pathsum = sourceVertex_val._1 + edge_weight;

                // append destination vertex to current source vertex traversed list
                List<Long> added_path = Lists.newArrayList();
                added_path.addAll(sourceVertex_val._2());
                added_path.add((Long)dstVertex._1());

                // message cuntaining the cost till destination vertex and traversing list till destination vertex
                Tuple2<Integer,List<Long>> message_dst = new Tuple2<Integer,List<Long>>(pathsum,added_path);

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer,List<Long>>>(triplet.dstId(),message_dst)).iterator()).asScala();
            }
        }
    }

    //scala.Function2<A,A,A> mergeMsg, A is of type Tuple2 here
    private static class merge extends AbstractFunction2<Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>,Tuple2<Integer,List<Long>>> implements Serializable {
        @Override
        // the logic is same in apply, whaterver messages are coming we just need to send the minimum one
        public Tuple2<Integer,List<Long>> apply(Tuple2<Integer,List<Long>> o, Tuple2<Integer,List<Long>> o2) {
            return null;
        }
    }

	public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();


        // A list of type Tuple2<Object,Tuple2<Integer,List<Long>> for saving the 
        // 1. vertex ID (Object)
        // 2. Tuple2<Integer,List<Long>> for saving the cost value and list of visited vertices before reaching to the current vertex
        // Initializing the source vertex as 0 and other as MAX VAlUE of INTEGER and array list as the vertex itself 
        List<Tuple2<Object,Tuple2<Integer,List<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(1l,new Tuple2<Integer,List<Long>>(0,Lists.newArrayList(1l))),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(2l,new Tuple2<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList(2l))),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(3l,new Tuple2<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList(3l))),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(4l,new Tuple2<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList(4l))),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(5l,new Tuple2<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList(5l))),
                new Tuple2<Object,Tuple2<Integer,List<Long>>>(6l,new Tuple2<Integer,List<Long>>(Integer.MAX_VALUE,Lists.newArrayList(6l)))
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );


        JavaRDD<Tuple2<Object,Tuple2<Integer,List<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        // initial message to be send to all node 
        Tuple2<Integer,List<Long>> ini_message = new Tuple2<>(Integer.MAX_VALUE, Lists.newArrayList(1l));

        // apply function changes -> ini_message(initial message), ClassTag of VD as Tuple.class
        Graph<Tuple2<Integer,List<Long>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),ini_message, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // function changes -> type of VD as Tuple2.class
        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        
     
        ops.pregel(ini_message, // as defined above
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class)) // message type is Tuple2 as well
            .vertices()
            .toJavaRDD().sortBy(f -> ((Tuple2<Object,Tuple2<Integer,List<Long>>>) f)._1, true, 0)
            .foreach(v -> {
                // for each vertex that we get
                Tuple2<Object,Tuple2<Integer,List<Long>>> vertex = (Tuple2<Object,Tuple2<Integer,List<Long>>>)v;
                // take out the cost and traversed path 
                Tuple2<Integer,List<Long>> cost_path = vertex._2();
                List<String> labeled_path = Lists.newArrayList();
                // save the path in list of string with actual labels that are present in lables map
                for(Long v__of_path:cost_path._2()){
                    labeled_path.add(labels.get(v__of_path));
                }
                System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+labeled_path + " with cost " +cost_path._1());
            });
	}
}
