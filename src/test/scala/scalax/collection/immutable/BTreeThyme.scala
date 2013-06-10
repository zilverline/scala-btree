package scalax.collection.immutable

import scala.collection.immutable.TreeSet
import scala.collection.immutable.HashSet

object BTreeThyme {
  val random = new util.Random(1233312)
  val values = Vector.tabulate(1000000)(_.asInstanceOf[java.lang.Integer])
  val shuffled = random.shuffle(values)

  lazy val th = new ichi.bench.Thyme(0.1)

  val DefaultSizes = Vector(1000, 1, 10, 100, 1000, 10000, 100000, 1000000)

  def containsSequential(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.values.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"contains $i sequential values")(xs.forall(btree.contains), ftitle = "btree")(xs.forall(ts.contains), htitle = "treeset")
  }

  def containsShuffled(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"contains $i shuffled values")(xs.forall(btree.contains), ftitle = "btree")(xs.forall(ts.contains), htitle = "treeset")
  }

  def insertSequential(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.values.take(i)
    th.pbenchOff(s"insert $i sequential values")(BTree(xs: _*).size, ftitle = "btree")(TreeSet(xs: _*).size, htitle = "treeset")
  }

  def insertShuffled(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    th.pbenchOff(s"insert $i shuffled values")(BTree(xs: _*).size, ftitle = "btree")(TreeSet(xs: _*).size, htitle = "treeset")
  }

  def insertShuffled2(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    th.pbenchOff(s"insert $i shuffled values")(BTree(xs: _*).size, ftitle = "btree")(HashSet(xs: _*).size, htitle = "hashset")
  }
}
