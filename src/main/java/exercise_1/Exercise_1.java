// package name
package exercise_1;

// importing packages
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {

    // serializable interface is used to store objects, to send on output streams, outside packages
    // serialization allows us to convert the state of an object into a byte stream, 
    // which then can be saved into a file on the local disk or sent over the network to any other machine
    // AbstractFunction3 scala class inheriting
    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        // Since Integer is a wrapper class for int data type, it gives us more flexibility in storing, converting and manipulating an int data
        // vertex ID - name of vertex
        // vertex value - value in node
        // message - passing values

        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            System.out.println("===================================================================================================");
            System.out.println("The vertex and message status");
            System.out.println("VID is -"+vertexID);
            System.out.println("Vvalue is - "+vertexValue);
            System.out.println("Messages is "+message);
            if (message == Integer.MAX_VALUE) {             // superstep 0
                System.out.println("for VID "+vertexID+" present state - "+Integer.MAX_VALUE+" and changed state after calculation is "+vertexValue);
                return vertexValue;
            } else {
                int m = Math.max(vertexValue,message);                                     // superstep > 0
                System.out.println("for VID "+vertexID+" present state - "+vertexValue+" and changed state after calculation is "+m);
                return Math.max(vertexValue,message);
            }
        }
    }


    // AbstractFunction1 scala class
    // EdgeTriplet<Integer,Integer> data type, source and destination vertex 
    // Iterator<Tuple2<Object,Integer>>> an itertor(pointer) 
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            // triplate is like ((2,1),(4,8),1), source dest and properties
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // do nothing
                System.out.println("No messages sent from VID "+sourceVertex._1+" to VID "+ dstVertex._1);
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                // send the message to destination vertex with value of source
                System.out.println("The messages sent from VID "+sourceVertex._1+" to VID "+ dstVertex._1 +" is "+sourceVertex._2);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2)).iterator()).asScala();
            }
        }
    }


    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            // System.out.println("merge message");
            // System.out.println(o+" "+o2);
            // if we face other condition then the above apply function we return null in that case, 
            return null;
        }
    }






    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
            new Tuple2<Object,Integer>(1l,9),
            new Tuple2<Object,Integer>(2l,1),
            new Tuple2<Object,Integer>(3l,6),
            new Tuple2<Object,Integer>(4l,8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
            new Edge<Integer>(1l,2l, 1),
            new Edge<Integer>(2l,3l, 1),
            new Edge<Integer>(2l,4l, 1),
            new Edge<Integer>(3l,4l, 1),
            new Edge<Integer>(3l,1l, 1)
        );
        
        //print verices
        System.out.println("The vertices are - ");
        System.out.println(vertices);

        //print edges
        System.out.println("The Edges are - ");
        System.out.println(edges);

        // seperator
        System.out.println("===================================================================================================");
 

        // ctx is the configratoin for working with java on spark, we create RDDs with 
        // vertices and edges, these are the nodes which spark create and work parallely
        // we create RDDs for vertex and edges to make work them parallelize    
        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);
        
        // we create graphs from the above vertex and edges with some predefined arguments for scala
        // scala.reflect.ClassTag$.MODULE$.apply(Integer.class) is used each for VD and ED, to get java objects 
        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // The extended function of graph, su
        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // pregel passing paramters as mentioned in lab assignment 
        Tuple2<Long,Integer> max = (Tuple2<Long,Integer>)ops.pregel(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,      // Run until convergence
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
        .vertices().toJavaRDD().first();
        System.out.println();
        System.out.println(max._2 + " is the maximum value in the graph");
	}
}
