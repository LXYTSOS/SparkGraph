package com.lxy.graph

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
 * @author sl169
 */
object BuildGraph {
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("BuildGraph").setMaster("local[*]").set("spark.driver.memory", "2G")
    val sc = new SparkContext(conf)
    
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
    
    //食物原料，id 原料名称  科目
    val ingredients:RDD[(VertexId, FNNode)] = sc.textFile("data/ingr_info.tsv").
      filter(!_.startsWith("#")).
      map{
        line => val row = line split '\t'
        (row(0).toInt, new Ingredient(row(1), row(2)))
      }
    
    //化合物，id 化合物名称  CAS编号
    val compounds:RDD[(VertexId, FNNode)] = sc.textFile("data/comp_info.tsv").
      filter(!_.startsWith("#")).
      map{
        line => val row = line split '\t'
        (10000L + row(0).toInt, new Compound(row(1), row(2)))
      }
    
    //关系，原料id 化合物id
    val links:RDD[Edge[Int]] = sc.textFile("data/ingr_comp.tsv").
      filter(!_.startsWith("#")).
      map{
        line => val row = line split '\t'
        Edge(row(0).toInt, 10000L + row(1).toInt, 1)
      }
    
    val nodes = ingredients ++ compounds
    
    val foodNetwork = Graph(nodes, links)
    foodNetwork.triplets.take(5).foreach(showTriplet _ andThen println _)
//    The ingredient calyptranthes_parriculata contains citral_(neral)
//    The ingredient chamaecyparis_pisifera_oil contains undecanoic_acid
//    The ingredient hyssop contains myrtenyl_acetate
//    The ingredient hyssop contains 4-(2,6,6-trimethyl-cyclohexa-1,3-dienyl)but-2-en-4-one
//    The ingredient buchu contains menthol
  }
  
  def showTriplet(t: EdgeTriplet[FNNode,Int]): String = "The ingredient " ++ t.srcAttr.name ++ " contains " ++ t.dstAttr.name
  
}

//这部分代码与书本有所不同，后两个class前面没有case，
//在上面的调用中如果直接写上类名的话会报没有可用的构造方法，
//所以上面使用new关键字，那么就把case去掉，
//此时还会报NotSerializableException异常，
//所以在父类上继承Serializable，在子类中使用with Serializable好像不行
class FNNode(val name: String) extends Serializable

class Ingredient(override val name:String,category:String) extends FNNode(name)

class Compound(override val name:String,cas: String) extends FNNode(name)