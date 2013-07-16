package scalax.collection.immutable

import scala.collection.immutable.{ HashSet, SortedSet, TreeSet }

object BTreeThyme {
  val random = new util.Random(1233312)
  val values = Vector.tabulate(1000000)(_.asInstanceOf[java.lang.Integer])
  val shuffled = random.shuffle(values)

  lazy val th = new ichi.bench.Thyme()

  val DefaultSizes = Vector(1000, 1, 10, 100, 1000, 10000, 100000, 1000000)
  val DefaultParameters = Vector(
    BTree.Parameters(6, 5),
    BTree.Parameters(8, 8),
    BTree.Parameters(12, 12),
    BTree.Parameters(14, 12),
    BTree.Parameters(16, 16))

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
    th.pbenchOff(s"insert $i sequential values")(xs.foldLeft(BTree.empty[Int])(_ + _).size, ftitle = "btree")(xs.foldLeft(TreeSet.empty[Int])(_ + _).size, htitle = "treeset")
  }

  def insertShuffled(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    th.pbenchOff(s"insert $i shuffled values")(xs.foldLeft(BTree.empty[Int])(_ + _).size, ftitle = "btree")(xs.foldLeft(TreeSet.empty[Int])(_ + _).size, htitle = "treeset")
  }

  def insertShuffled2(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    th.pbenchOff(s"insert $i shuffled values")(BTree(xs: _*).size, ftitle = "btree")(HashSet(xs: _*).size, htitle = "hashset")
  }

  def deleteSequential(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.values.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"delete $i sequential values")(xs.foldLeft(btree)(_ - _).isEmpty, ftitle = "btree")(xs.foldLeft(ts)(_ - _).isEmpty, htitle = "treeset")
  }

  def deleteShuffled(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"delete $i shuffled values")(xs.foldLeft(btree)(_ - _).isEmpty, ftitle = "btree")(xs.foldLeft(ts)(_ - _).isEmpty, htitle = "treeset")
  }

  def iterateShuffled(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"iterate $i shuffled values")(btree.iterator.size, ftitle = "btree")(ts.iterator.size, htitle = "treeset")
  }

  def iterateSequential(sizes: Seq[Int] = DefaultSizes): Unit = sizes foreach { i =>
    val xs = BTreeThyme.values.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"iterate $i sequential values")(btree.iterator.size, ftitle = "btree")(ts.iterator.size, htitle = "treeset")
  }

  def insertShuffledVaryingOrder(sizes: Seq[Int] = DefaultSizes, parameters: Seq[BTree.Parameters] = DefaultParameters): Unit = for {
    size <- sizes
    (p1, p2) <- parameters.zip(parameters.tail)
  } {
    val xs = BTreeThyme.shuffled.take(size)
    th.pbenchOff(s"insert $size shuffled values")(BTree(xs: _*)(implicitly, p1).size, ftitle = s"btree L=${p1.minLeafValues},I=${p1.minInternalValues}")(BTree(xs: _*)(implicitly, p2).size, htitle = s"btree L=${p2.minLeafValues},I=${p2.minInternalValues}")
  }

  def insertShuffledVaryingOrder2(sizes: Seq[Int] = DefaultSizes, parameters: Seq[BTree.Parameters] = DefaultParameters): Unit = for {
    size <- sizes
    p1 <- parameters
  } {
    val xs = BTreeThyme.shuffled.take(size)
    th.pbench(BTree(xs: _*)(implicitly, p1).size, title = s"btree L=${p1.minLeafValues},I=${p1.minInternalValues},N=${size}")
  }

  private def simpleBench[A](label: String, f: SortedSet[Integer] => A): Unit = DefaultSizes foreach { i =>
    val xs = BTreeThyme.shuffled.take(i)
    val btree = BTree(xs: _*)
    val ts = TreeSet(xs: _*)
    th.pbenchOff(s"$label of $i shuffled values")(f(btree), ftitle = "btree")(f(ts), htitle = "treeset")
  }

  def benchHead() = simpleBench("head", _.head)
  def benchTail() = simpleBench("tail", _.tail)
  def benchLast() = simpleBench("last", _.last)
  def benchInit() = simpleBench("init", _.init)
}
