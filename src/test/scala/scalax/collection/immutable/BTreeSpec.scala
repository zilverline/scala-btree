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

A non-empty B-Tree should
  not contain deleted element                        $notContainDeletedElement
  not contain any elements after all are deleted     $deleteAllElements

Counter-examples
  ${example(-1, -2, 2, 3, -3, 5, 6, 7, 1, -4, 8, 4, 9, 0, -5, 10, -6)}
  ${example(2, 3, 1, -1, -2, 4, -5, 5, -6, 6, -7, 0, -3, 7, -4, 8, 9)}
  ${example(-2, -1, 5, 2, -3, 6, 7, 0, 1, -4, -5, -6, -7, 3, -8, -9, 4)}
  ${example(6, 7, -1, 8, -2, 0, 9, 2, 10, 3, -3, 11, -4, -5, 4, 5, 1)}

Delete counter-examples
  ${delete(0, 1, -1, -2, 2)(0)}
  ${delete(-1, 0, 1, -2, 2)(-1)}
  ${delete(-1, -3, -2, 0, 1)(0)}
  ${delete(-1, 1, -2, 0, 2, 3)(0)}
  ${delete(0, -1, -2, 1, 3, 2)(3)}
  ${delete(0, -1, 2, -3, -2, 1)(-3)}
  ${delete(0, -1, 4, 1, 5, 2, 3, 6)(4)}
  ${delete(0, -1, 1, 2, -2, 5, 3, 4)(5)}
  ${delete(-2, -3, -1, 0, 1, -4, 2)(-1)}
  ${delete(-1, -2, 2, -3, 0, 1, 3, 4, -4)(0)}
  ${delete(2, -2, 3, 1, -3, -1, -4, 0)(-4)}
  ${delete(-2, 0, 4, -3, -8, -9, 1, -10, 5, 6, -4, -5, -6, -7, -1, 2, 3)(0)}
  ${delete(-5, -6, 2, -7, -2, 3, 0, 1, -8, 4, -1, -3, 5, -9, -10, -4, 6)(1)}
  ${delete(-2, 0, -3, 2, 3, 4, -1, -4, 5, -5, -6, 1, 6, 7, 8, -7, -8)(0)}
  ${delete(-6, 0, -7, -3, -8, 1, -9, -4, -5, 3, 4, 5, -10, -11, 2, -1, -2)(1)}
  ${delete(-2, 2, 0, 3, -1, 1, -3, 4, 5, 9, -4, 10, 11, -5, 6, 7, 8)(0)}
  ${delete(-1, -2, 4, 1, 2, -5, 8, -6, 3, -7, 0, 5, 6, 9, -3, -4, 7)(0)}
  ${delete(-2, 2, -3, 0, 6, -4, 3, 7, -1, 8, -10, 9, 10, 4, 11, 1, 5)(-10)}
  ${delete(-2, 9, -5, 2, 1, 3, -6, 8, -1, 10, 4, 5, -7, 0, -3, 6, -4, 7)(8)}
  ${delete(-2, -4, -5, -6, 0, 2, 3, 4, -1, 1, 5, -3, -7, -10, 6, -8, 7, -9)(-10)}
  ${delete(0, 7, 1, 8, 9, 13, 10, 2, -2, 11, 3, 4, -3, -4, 5, 6, -1, 12)(13)}
  ${delete(6, 7, 8, 4, -2, 0, 5, -5, 2, -3, -4, -6, 3, -7, -8, 1, -9, -1)(0)}
  ${delete(-3, -2, -4, 0, 6, -1, -5, -6, 2, 3, -7, 1, 4, 5, -8, -9, 7, -10)(0)}
  ${delete(4, 7, 0, -1, -2, 2, 8, 9, -3, -4, 5, 6, 10, 1, 11, 12, -5, 3)(0)}
  ${delete(2, -5, 3, -6, -7, 8, 9, 10, 4, -2, -3, -4, 5, -1, 1, 6, 7, 0)(0)}
  ${delete(2, -2, -5, 1, 3, -10, 4, -1, -6, 5, 6, -7, -3, -8, 0, 7, -4, -9, 8, 9)(0)}
  ${delete(-1, 2, 7, -2, -3, -7, -8, 3, -9, -10, 5, -11, 6, -4, 0, -5, 4, -12, -6, 1)(0)}
  ${delete(-2, -5, -6, -7, 1, 2, -3, 3, -8, 7, 8, -4, 9, -1, 0, 4, 6, -9, 5, -10)(0)}
  ${delete(-1, -6, -7, 1, -8, 0, 2, -9, -2, 3, -10, 4, 5, -11, 6, 7, -3, 8, -4, -5)(0)}
  ${delete(1, -4, 2, -5, -6, -1, -2, -3, -10, -11, 3, -12, -7, -13, -8, -9, 4, 0, -14, -15)(0)}
  ${delete(-1, -2, 2, -3, 3, 5, 6, 7, 8, 1, 9, -4, 10, 0, 11, 4, -5, -6, 12, 13)(0)}
  ${delete(2, 1, 7, -2, -3, -5, -6, 8, 3, 9, -7, -4, 4, 5, 10, 11, 12, 0, -1, 6)(1)}
  ${delete(-5, 1, 2, -6, -11, -12, -7, -13, -8, -9, -1, -14, -2, 3, 0, -3, -4, -10, -16, -15)(-16)}
  ${delete(-2, 2, 0, -3, 3, -6, 4, -7, 1, -8, -4, -9, -1, 5, 6, -5, 7, -10, -11, 8)(0)}
  ${delete(-2, -3, 4, 5, 6, 7, 8, 2, -4, 9, 0, 10, 11, 12, -5, 3, -1, -6, 1, -7)(1)}
  ${delete(-1, -2, 2, 3, 0, 4, -9, 5, -1425017311, -1425017308, -6, -1425017309, 1, -1425017312, -7, -1425017313, -1425017307, -8, -10, -1425017310, -11, -3, -12, -1425017314, -1425017315, -4, -5, -1425017316, -13)(-1425017307)}
  ${delete(7, 8, 2, 9, -1, -31871744, 10, -31871738, -31871739, -31871745, -31871740, -31871746, 3, -31871747, -31871735, 11, 1, 4, -31871741, -31871736, -31871737, 5, -31871742, -31871748, 6, 0, 12, -31871743, -31871749)(-31871735)}
  ${delete(1585351458, 1585351459, -2, 1, -5, -6, 2, 3, -1, -7, -3, 1585351457, 4, -8, 5, 6, -9, -10, 0, -4)(1585351457)}
  ${delete(-2, -3, 11, 12, -1, 0, 5, 13, -4, -5, -6, -7, 6, 14, -8, 7, 2, 15, -9, 3, 8, 1, 9, 4, -10, 10, 16, -11, -12)(0)}

Range examples
  ${range(0, 1, -1, 2, -2)(0)}
  ${range(1, 2, -1, 3, 0)(0)}
  ${range(0, -1, 1, -2, 2)(1)}
  ${range(-1, 2, 0, 1, 3)(-1)}
  ${range(1, -1, 0, 2, 3, 4)(0)}
  ${range(0, -1, 1, -2, 2)(3)}
  ${range(0, -1, 1, 2, -2, -3, -4, -5)(1)}
  ${range(0, 1, -1, 2, -2, 3, 4, 5)(0)}
  ${range(-2, 2, 1, -1, -3, 8, 0, 9, -4, 3, 4, -5, 10, 5, -6, 6, 7)(0)}
  ${range(2, -4, 1, -5, 3, -6, 0, -7, -8, 4, 5, -2, -9, -10, -1, 6, -3)(0)}
  ${range(-3, -4, -6, -7, -8, -9, -1, -5, 0, 1, -10, 2, -2, 3, 4, -11, 5)(0)}
  ${range(-1, 2, -2, 1, -3, 3, 0, 4, 5, 6, -4, 7, 8, 9, 10, -5, -6)(-3)}
  ${range(0, -2, -1, 2, 3, -3, -4, -5, -6, 1, 4, -7, 5, 6, 7, -8, 8)(6)}
  ${range(-2, -1, 0, 2, 6, 7, 8, -3, 3, -4, 4, -5, -6, 5, -7, 1, -8)(-8)}
  ${range(6, 7, 8, -2, 2, 3, 0, -3, 1, -6, -7, -8, 4, -4, -1, -5, 5, 9, 10, 11)(-5)}
  ${range(-12, -13, 0, 1, -6, -2, -14, 2, -1, -7, -8, -15, -9, -3, -4, -5, -16, -10, -17, -11)(0)}
  ${range(0, 6, -2, 7, -6, -7, -3, 2, 3, 8, 4, 9, -8, -4, -5, 1, -1, 10, 11, 5, 12, 13, 14)(-2)}
  ${range(-9, -10, -11, -3, 3, -12, 4, -4, -5, 1, -1, 5, -13, 0, -14, 2, -6, -2, -7, -8, -15, -16, -17)(0)}
  """

  implicit val params = Parameters(minTestsOk = 1000, minSize = 0, maxSize = 2000, workers = Runtime.getRuntime().availableProcessors())

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
    BTree(elements: _*).toVector must (beSorted[Int] and containTheSameElementsAs(elements.distinct))
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

  def example(elts: Int*) = BTree(elts: _*).toVector must_== Vector(elts: _*).distinct.sorted

  def delete(elts: Int*)(valueToDelete: Int) = {
    val actual = BTree(elts: _*) - valueToDelete
    val expected = TreeSet(elts: _*) - valueToDelete
    actual.toSeq must (beSorted[Int] and containTheSameElementsAs(expected.toSeq))
  }

  def range(elts: Int*)(valueToSplit: Int) = {
    val actual = BTree(elts: _*)
    val expected = TreeSet(elts: _*)
    (actual.until(valueToSplit).toSeq ++ actual.from(valueToSplit).toSeq) must (beSorted[Int] and containTheSameElementsAs(expected.toSeq))
  }
}
