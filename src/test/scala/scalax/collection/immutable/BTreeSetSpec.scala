package scalax.collection.immutable

import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.matcher.Parameters
import scala.collection.immutable.TreeSet

class BTreeSetSpec extends org.specs2.Specification with org.specs2.ScalaCheck {
  implicit val parameters = BTreeSet.Parameters(minLeafValues = 2, minInternalValues = 2)

  private def tree[A: Ordering](a: A*): BTreeSet[A] = a.foldLeft(BTreeSet.withParameters[A](parameters))(_ + _)

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
  iterate from element in order                      $iterateFromInOrder
  support splitting                                  $splittable
  head/tail identity                                 $headTailIdentity
  init/last identity                                 $initLastIdentity
  support take                                       $take
  support drop                                       $drop
  take/drop identity                                 $takeDropIdentity
  support splitAt                                    $splitAt
  support slice                                      $slice

A non-empty B-Tree should
  not contain deleted element                        $notContainDeletedElement
  not contain any elements after all are deleted     $deleteAllElements
  """

  implicit val params = Parameters(minTestsOk = 1000, minSize = 0, maxSize = 100, workers = Runtime.getRuntime().availableProcessors())

  implicit def GenTree[T: Arbitrary: Ordering]: Arbitrary[BTreeSet[T]] = Arbitrary(for {
    elements <- Gen.listOf(arbitrary[T])
  } yield tree(elements: _*))

  def empty = BTreeSet.empty[Int]
  def singleton = tree(1)
  def pair = tree(2, 1)

  def beEmpty = empty.isEmpty must beTrue
  def haveLength0 = empty.size must_== 0

  def singletonNotEmpty = singleton.nonEmpty must beTrue
  def singletonSize1 = singleton must have size(1)
  def singletonContains1 = singleton must contain(1)

  def pairContainsElementsInOrder = pair.toVector must beEqualTo(Vector(1, 2))

  def containsAllInsertedElements = Prop.forAll { elements: List[Int] =>
    val subject = tree(elements: _*)
    elements.forall(subject.contains)
  }
  def containAllDistinctElements = Prop.forAll { elements: List[Int] =>
    tree(elements: _*) must have size(elements.distinct.size)
  }
  def containsElementsInOrder = Prop.forAll { elements: List[Int] =>
    tree(elements: _*).toVector must_== elements.sorted.distinct.toVector
  }
  def iterateElementsInOrder = Prop.forAll { elements: List[Int] =>
    tree(elements: _*).iterator.toVector must_== elements.toVector.distinct.sorted
  }
  def iterateFromInOrder = Prop.forAll(GenListAndKey[Int]) {
    case (elements, key) =>
    val subject = tree(elements: _*)
    subject.iteratorFrom(key).toVector must_== subject.from(key).iterator.toVector
  }
  def headTailIdentity = Prop.forAll { subject: BTreeSet[Int] => subject.nonEmpty ==> {
    subject must_== (subject.tail + subject.head)
  }}
  def initLastIdentity = Prop.forAll { subject: BTreeSet[Int] => subject.nonEmpty ==> {
    subject must_== (subject.init + subject.last)
  }}

  def GenNonEmptyTreeWithSelectedElement[T: Arbitrary: Ordering] = for {
    elements <- Gen.listOf(arbitrary[T])
    if elements.nonEmpty
    toBeDeleted <- Gen.oneOf(elements)
  } yield (elements, toBeDeleted)

  def GenListAndKey[T: Arbitrary] = for {
    elements <- arbitrary[List[T]]
    key <- Gen.oneOf(arbitrary[T], Gen.oneOf(elements))
  } yield (elements, key)

  def GenTreeWithSelectedIndex[T: Arbitrary: Ordering] = for {
    elements <- Gen.listOf(arbitrary[T])
    index <- Gen.choose(-1, elements.size + 1)
  } yield (elements, index)

  def notContainDeletedElement = Prop.forAll(GenNonEmptyTreeWithSelectedElement[Int]) {
    case (elements, toBeDeleted) =>
      val initial = tree(elements: _*)
      val deleted = initial - toBeDeleted
      (deleted.size must_== (initial.size - 1)) and (deleted.contains(toBeDeleted) aka "value still present" must beFalse) and (deleted.toVector must_== TreeSet(elements: _*).-(toBeDeleted).toVector)
  }
  def deleteAllElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    elements.nonEmpty ==> {
      val subject = tree(elements: _*)
      elements.foldLeft(subject)(_ - _).toVector must_== Vector.empty
    }
  }

  def splittable = Prop.forAll(GenListAndKey[Int]) {
    case (elements, key) =>
      val initial = tree(elements: _*)
      val init = initial.until(key)
      val tail = initial.from(key)
      (init must contain(be_<(key)).forall) and
        (tail must contain(be_>=(key)).forall) and
        ((init.toVector ++ tail.toVector) must (beSorted[Int] and containTheSameElementsAs(elements.distinct)))
  }

  def take = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = tree(elements: _*)
      subject.take(n).toVector must_== subject.toVector.take(n)
  }

  def drop = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = tree(elements: _*)
      subject.drop(n).toVector must_== subject.toVector.drop(n)
  }

  def takeDropIdentity = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = tree(elements: _*)
      (subject.take(n) ++ subject.drop(n)) must_== subject
  }

  def splitAt = Prop.forAll(GenTreeWithSelectedIndex[Int]) {
    case  (elements, n) =>
      val subject = tree(elements: _*)
      val (left, right) = subject.splitAt(n)
      (left must_== subject.take(n)) and (right must_== subject.drop(n))
  }

  def slice = Prop.forAll(for {
    elements <- arbitrary[List[Int]]
    from <- Gen.choose(-1, elements.size + 1)
    until <- Gen.choose(-1, elements.size + 1)
  } yield (elements, from, until)) {
    case (elements, from, until) =>
      tree(elements: _*).slice(from, until).toVector must_== TreeSet(elements: _*).slice(from, until).toVector
  }
}
