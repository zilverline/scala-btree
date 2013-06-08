package scalax.collection.immutable

object BTreeThyme {
  val random = new util.Random(1233312)
  val values = Vector.tabulate(1000000)(_.asInstanceOf[java.lang.Integer])
  val shuffled = random.shuffle(values)

  def makeBTree = BTree(values: _*)

  def makeTreeSet = collection.immutable.TreeSet(values: _*)
}
