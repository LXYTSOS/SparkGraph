package com.lxy.graph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.math.abs
import breeze.linalg.SparseVector
import org.apache.spark.graphx.GraphLoader

/**
 * @author sl169
 */
object BuildGraph {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BuildGraph").setMaster("local[*]").set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)

    //构建有向图

    //    val emailGraph = GraphLoader.edgeListFile(sc, "data/Email-Enron.txt")
    //    emailGraph.vertices.take(5).foreach(println)
    //    emailGraph.edges.take(5).foreach(println)

    //获取某个顶点（srcId）指向的所有目标顶点（dstId）
    //    val dst = emailGraph.edges.filter(_.srcId == 19021).map(_.dstId).collect()
    //    for(d <- dst){
    //      print(d+" ")
    //    }
    //获取指向某个顶点（dstId）的所有源顶点（srcId）
    //    val src = emailGraph.edges.filter(_.dstId == 19021).map(_.srcId).collect()
    //    for(s <- src){
    //      print(s+" ")
    //    }

    //计算邮件网络某个节点的入度和出度
    //    println(emailGraph.numEdges)
    //367662
    //    println(emailGraph.numVertices)
    //36692
    //    println(emailGraph.inDegrees.map(_._2).sum / emailGraph.numVertices)
    //10.020222391802028
    //    println(emailGraph.outDegrees.map(_._2).sum / emailGraph.numVertices)
    //10.020222391802028

    //计算发出邮件最多的
    //    println(emailGraph.outDegrees.reduce(max))
    //(5038,1383)

    //    println(emailGraph.outDegrees.filter(_._2 <= 1).count)
    //11211

    //构建二分图/两偶图

    //食物原料，id 原料名称  科目
//    val ingredients: RDD[(VertexId, FNNode)] = sc.textFile("data/ingr_info.tsv").
//      filter(!_.startsWith("#")).
//      map {
//        line =>
//          val row = line split '\t'
//          (row(0).toInt, new Ingredient(row(1), row(2)))
//      }

    //化合物，id 化合物名称  CAS编号
//    val compounds: RDD[(VertexId, FNNode)] = sc.textFile("data/comp_info.tsv").
//      filter(!_.startsWith("#")).
//      map {
//        line =>
//          val row = line split '\t'
//          (10000L + row(0).toInt, new Compound(row(1), row(2)))
//      }

    //关系，原料id 化合物id
//    val links: RDD[Edge[Int]] = sc.textFile("data/ingr_comp.tsv").
//      filter(!_.startsWith("#")).
//      map {
//        line =>
//          val row = line split '\t'
//          Edge(row(0).toInt, 10000L + row(1).toInt, 1)
//      }
//
//    val nodes = ingredients ++ compounds

//    val foodNetwork = Graph(nodes, links)
    //        foodNetwork.triplets.take(5).foreach(showTriplet _ andThen println _)
    //    The ingredient calyptranthes_parriculata contains citral_(neral)
    //    The ingredient chamaecyparis_pisifera_oil contains undecanoic_acid
    //    The ingredient hyssop contains myrtenyl_acetate
    //    The ingredient hyssop contains 4-(2,6,6-trimethyl-cyclohexa-1,3-dienyl)but-2-en-4-one
    //    The ingredient buchu contains menthol

//    val mostOut = foodNetwork.outDegrees.reduce(max)
//    println(mostOut)
//    //(908,239)
//    val mostOutArr = foodNetwork.vertices.filter(_._1 == mostOut._1).collect()
//    val mostOutName = mostOutArr.map { out =>
//      val n = out._2
//      n.name
//    }
//    mostOutName.foreach(println)
    //black_tea

//    val mostIn = foodNetwork.inDegrees.reduce(max)
//    println(mostIn)
//    //(10292,299)
//    val mostInArr = foodNetwork.vertices.filter(_._1 == mostIn._1).collect()
//    val mostInName = mostInArr.map { in =>
//      val n = in._2
//      n.name
//    }
//    mostInName.foreach(println)
    //1-octanol

    //构建加权社交网络

        type Feature = breeze.linalg.SparseVector[Int]

        val featureMap: Map[Long, Feature] = Source.fromFile("data/ego.feat").
          getLines().
          map {
            line =>
              val row = line split ' '
              val key = abs(row.head.hashCode.toLong)
              val feat = SparseVector(row.tail.map(_.toInt))
              (key, feat)
          }.toMap

        val edges: RDD[Edge[Int]] = sc.textFile("data/ego.edges").
          map {
            line =>
              val row = line split ' '
              val srcId = abs(row(0).hashCode.toLong)
              val dstId = abs(row(0).hashCode.toLong)
              val srcFeat = featureMap(srcId)
              val dstFeat = featureMap(dstId)
              //点乘：相应元素相乘然后全部相加
              val numCommonFeats = srcFeat dot dstFeat
              Edge(srcId, dstId, numCommonFeats)
          }

        val egoNetwork: Graph[Int, Int] = Graph.fromEdges(edges, 1)

    //    println(egoNetwork.edges.filter(_.attr == 3).count())
    //    println(egoNetwork.edges.filter(_.attr == 2).count())
    //    println(egoNetwork.edges.filter(_.attr == 1).count())
        
        //查看网络的最大最小度
//        println(egoNetwork.degrees.reduce(max))
        //(1643293729,1852)
//        println(egoNetwork.degrees.reduce(min))
        //(254839883,2)
        
        //度的直方图
        val histogram = egoNetwork.degrees.
          map(t => (t._2, t._1)).
          groupByKey.map(t => (t._1, t._2.size)).
          sortBy(_._1).collect()
        
        histogram.take(10).foreach(print)
        //(2,29)(4,33)(6,34)(8,21)(10,27)(12,16)(14,10)(16,10)(18,15)(20,12)
  }

  def showTriplet(t: EdgeTriplet[FNNode, Int]): String = "The ingredient " ++ t.srcAttr.name ++ " contains " ++ t.dstAttr.name

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }
  
  def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 < b._2) a else b
  }

}

//这部分代码与书本有所不同，后两个class前面没有case，
//在上面的调用中如果直接写上类名的话会报没有可用的构造方法，
//所以上面使用new关键字，那么就把case去掉，
//此时还会报NotSerializableException异常，
//所以在父类上继承Serializable，在子类中使用with Serializable好像不行
class FNNode(val name: String) extends Serializable

class Ingredient(override val name: String, category: String) extends FNNode(name)

class Compound(override val name: String, cas: String) extends FNNode(name)