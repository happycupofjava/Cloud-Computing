

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@SerialVersionUID(123L)
case class Vertex ( cid: Long, vid: Long, op:List[String])
extends Serializable{}

object Partition {

  val depth = 6

  def main ( args: Array[ String ] ) {
      var count: Int =1;
      //val ip= Vertex(cid,vid,op)
      val conf = new SparkConf().setAppName("Map1").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val firstm = sc.textFile(args(0)).map( line => {
       var input = line.split(",");
       var vid = input(0).toLong;
       var op = input.toList.tail;

       var cid: Long =0;
       if(count <= 5)
       {
          cid= vid;

       }
       else
       {
           cid= -1;
       }
       count= count+1    ;
       Vertex(cid,vid,op)    
      
      })
      //var d = sc.collect(firstm)
      //firstm.foreach(println)

      var firststr = firstm.map( firstm => (firstm.vid, firstm))
      .map{ case (k, firstm) =>
      var f_vid = firstm.vid;
      var f_cid = firstm.cid;
      var f_op = firstm.op;
      (f_vid, Vertex(f_cid, f_vid, f_op))
      }
      //firststr.foreach(println)

      var secondm1 = firststr.values.map(firststr => (firststr.vid, firststr))
      var secondm2 = firststr.flatMap(firststr => for (j <- firststr._2.op)yield{
      	(j.toLong, new Vertex(firststr._2.cid, 0.toLong,List(0.toString)))
      })
      var secondm = secondm1.union(secondm2).groupByKey()
      //secondm.collect.foreach(println)
      

      var secondr = secondm.map(secondm => 
      	{
      		var max = Long.MaxValue;
      		var list1 : List[String] = List()
      		for (i <- secondm._2){
                        list1 = i.op

      			if(max > i.cid)
      			{
      				max = i.cid
      			}
      		}
      		(max, Vertex(max, secondm._1, list1))
      	})
      firststr = secondr

      secondm1 = firststr.values.map(firststr => (firststr.vid, firststr))
      secondm2 = firststr.flatMap(firststr => for (j <- firststr._2.op) yield{
      	(j.toLong, new Vertex(firststr._2.cid, 0.toLong, List(0.toString)))
      })
      secondm = secondm1.union(secondm2).groupByKey()
      //secondm.collect.foreach(println)


      secondr = secondm.map(secondm => {
            var max = Long.MaxValue;
            var list1: List[String] = List()
            for (i <- secondm._2){
                  //println("*")

                  list1 = i.op
                  if(max > i.cid)
                  {
                        max = i.cid
                  }
            }
            (max, Vertex( max, secondm._1, list1))
      })

      firststr = secondr
      secondm1 = firststr.values.map( firststr => (firststr.vid, firststr))
      second2 = firststr.flatMap(firststr =. for (j <- firststr._2.op)yield {
            (j.toLong, new Vertex(firststr._2.cid, 0.toLong, List(0.toString)))
      })

      secondm = secondm1.union(secondm2).groupByKey()

      secondr= secondm.map(secondm => 
            {
                  var max = Long.MaxValue;
                  var list1 : List[String] = List()
                  for (i <- second._2){
                        list1.i.op
                        if(max > i.cid){
                              max = i.cid

                        }
                  }
                  (max, Vertex(firststr._2.cid, o.toLong, List(0.toString)))
            })
      secondm = secondm1.union(secondm2).groupByKey()

      secondr = secondm.map (secondm =>
            {
                  var max = Long.MaxValue;
                  car list1: List[String] = List()
                  for (i <- secondm._2){
                        list1 = i.op
                        if (max > i.cid)
                        {
                              max = i.cid
                        }
                  }
                  (max, Vertex (max, secondm._1,1.toLong))
            })

      var map3 = secondr.map(secondr => (secondr._2,1.toLong)).reduceByKey(_+_)

      map3.collect.foreach(println)
       sc.stop()

  }
}