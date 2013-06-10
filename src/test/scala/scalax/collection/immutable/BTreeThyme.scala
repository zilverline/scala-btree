package scalax.collection.immutable

import scala.collection.immutable.TreeSet

object BTreeThyme {
  val random = new util.Random(1233312)
  val values = Vector.tabulate(1000000)(_.asInstanceOf[java.lang.Integer])
  val shuffled = random.shuffle(values)

  lazy val th = ichi.bench.Thyme.warmed(verbose = print)

  val sizes = Vector(1, 10, 100, 1000, 10000, 100000, 1000000)

  def insertShuffled(): Unit = {
    sizes foreach { i =>
      val xs = BTreeThyme.shuffled.take(i);
      val btree = BTree(xs:_*);
      val ts = TreeSet(xs:_*);
      th.pbenchOff(s"contains $i shuffled values")(xs.forall(btree.contains), ftitle="btree")(xs.forall(ts.contains) ,htitle="treeset") }
  }

  def makeBTree = BTree(values: _*)

  def makeTreeSet = collection.immutable.TreeSet(values: _*)
}
