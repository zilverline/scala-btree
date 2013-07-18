package scalax.collection.immutable

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.matcher.Parameters
import scala.collection.immutable.TreeSet

class BTreeSpec extends org.specs2.Specification with org.specs2.ScalaCheck {
  implicit val parameters = BTree.Parameters(minLeafValues = 2, minInternalValues = 2)

  def is = s2"""
In-memory B-Tree specification

The empty B-Tree should
  be empty                                           $beEmpty
  have length 0                                      $haveLength0

Singleton B-Tree should
  not be empty                                       $singletonNotEmpty
  have size 1                                        $singletonSize1
  contain single element                             $singletonContains1

Two-element B-Tree should
  contain both elements in order                     $pairContainsElementsInOrder

A B-Tree should
  contain all inserted elements                      $containsAllInsertedElements
  contain all distinct elements                      $containAllDistinctElements
  contain all elements in order                      $containsElementsInOrder
  iterate all elements in order                      $iterateElementsInOrder
  support splitting                                  $splittable
  head/tail identity                                 $headTailIdentity
  init/last identity                                 $initLastIdentity
  support take                                       $take
  support drop                                       $drop
  take/drop identity                                 $takeDropIdentity
  support splitAt                                    $splitAt

A non-empty B-Tree should
  not contain deleted element                        $notContainDeletedElement
  not contain any elements after all are deleted     $deleteAllElements
  """

  implicit val params = Parameters(minTestsOk = 100, minSize = 0, maxSize = 100, workers = Runtime.getRuntime().availableProcessors())

  implicit def GenTree[T: Arbitrary: Ordering]: Arbitrary[BTree[T]] = Arbitrary(for {
    elements <- Gen.listOf(arbitrary[T])
  } yield BTree(elements: _*))

  def empty = BTree.empty[Int]
  def singleton = BTree(1)
  def pair = BTree(2, 1)

  def beEmpty = empty.isEmpty must beTrue
  def haveLength0 = empty.size must_== 0

  def singletonNotEmpty = singleton.nonEmpty must beTrue
  def singletonSize1 = singleton must have size(1)
  def singletonContains1 = singleton must contain(1)

  def pairContainsElementsInOrder = pair.toVector must beEqualTo(Vector(1, 2))

  def containsAllInsertedElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    val tree = BTree(elements: _*)
    elements.forall(tree.contains)
  }
  def containAllDistinctElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    BTree(elements: _*) must have size(elements.distinct.size)
  }
  def containsElementsInOrder = Prop.forAll(arbitrary[List[Int]]) { elements =>
    BTree(elements: _*).toVector must_== elements.sorted.distinct.toVector
  }
  def iterateElementsInOrder = Prop.forAll(arbitrary[List[Int]]) { elements =>
    BTree(elements: _*).iterator.toVector must (beSorted[Int] and containTheSameElementsAs(elements.distinct))
  }
  def headTailIdentity = Prop.forAll { subject: BTree[Int] => subject.nonEmpty ==> {
    subject must_== (subject.tail + subject.head)
  }}
  def initLastIdentity = Prop.forAll { subject: BTree[Int] => subject.nonEmpty ==> {
    subject must_== (subject.init + subject.last)
  }}

  def GenNonEmptyTreeWithSelectedElement[T: Arbitrary: Ordering] = for {
    elements <- Gen.listOf(arbitrary[T])
    if elements.nonEmpty
    toBeDeleted <- Gen.oneOf(elements)
  } yield (elements, toBeDeleted)

  def GenTreeWithSelectedIndex[T: Arbitrary: Ordering] = for {
    elements <- Gen.listOf(arbitrary[T])
    index <- Gen.choose(-1, elements.size + 1)
  } yield (elements, index)

  def notContainDeletedElement = Prop.forAll(GenNonEmptyTreeWithSelectedElement[Int]) {
    case (elements, toBeDeleted) =>
      val initial = BTree(elements: _*)
      val deleted = initial - toBeDeleted
      (deleted.size must_== (initial.size - 1)) and (deleted.contains(toBeDeleted) aka "value still present" must beFalse) and (deleted.toVector must_== TreeSet(elements: _*).-(toBeDeleted).toVector)
  }
  def deleteAllElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    elements.nonEmpty ==> {
      val tree = BTree(elements: _*)
      elements.foldLeft(tree)(_ - _).toVector must_== Vector.empty
    }
  }

  def splittable = Prop.forAll(GenNonEmptyTreeWithSelectedElement[Int]) {
    case (elements, key) =>
      val initial = BTree(elements: _*)
      val init = initial.until(key)
      val tail = initial.from(key)
      (init must contain(be_<(key)).forall) and
        (tail must contain(be_>=(key)).forall) and
        ((init.toVector ++ tail.toVector) must (beSorted[Int] and containTheSameElementsAs(elements.distinct)))
  }

  def take = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = BTree(elements: _*)
      subject.take(n).toVector must_== subject.toVector.take(n)
  }

  def drop = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = BTree(elements: _*)
      subject.drop(n).toVector must_== subject.toVector.drop(n)
  }

  def takeDropIdentity = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = BTree(elements: _*)
      (subject.take(n) ++ subject.drop(n)) must_== subject
  }

  def splitAt = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = BTree(elements: _*)
      val (left, right) = subject.splitAt(n)
      (left must_== subject.take(n)) and (right must_== subject.drop(n))
  }
}
