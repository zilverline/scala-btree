package scalax.collection.immutable

import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop
import org.scalacheck.Arbitrary
import org.specs2.matcher.Parameters

class BTreeSpec extends org.specs2.Specification with org.specs2.ScalaCheck {
  def is = s2"""
In-memory B-Tree specification

The empty B-Tree should
  be empty                           $beEmpty
  have length 0                      $haveLength0

Singleton B-Tree should
  not be empty                       $singletonNotEmpty
  have size 1                        $singletonSize1
  contain single element             $singletonContains1

Two-element B-Tree should
  contain both elements in order     $pairContainsElementsInOrder

A B-Tree should
  contain all inserted elements      $containsAllInsertedElements
  contain all distinct elements      $containAllDistinctElements
  contain all elements in order      $containsElementsInOrder

Counter-examples
  ${example(-1, -2, 2, 3, -3, 5, 6, 7, 1, -4, 8, 4, 9, 0, -5, 10, -6)}
  ${example(2, 3, 1, -1, -2, 4, -5, 5, -6, 6, -7, 0, -3, 7, -4, 8, 9)}
  ${example(-2, -1, 5, 2, -3, 6, 7, 0, 1, -4, -5, -6, -7, 3, -8, -9, 4)}
  ${example(6, 7, -1, 8, -2, 0, 9, 2, 10, 3, -3, 11, -4, -5, 4, 5, 1)}
  """

  implicit val params = Parameters(minTestsOk = 1000, minSize = 0, maxSize = 2000)

  implicit def GenTree[T: Arbitrary: Ordering]: Arbitrary[BTree[T]] = Arbitrary(for {
    elements <- Gen.listOf(arbitrary[T])
  } yield BTree(elements: _*))

  def empty = BTree.empty[Int]
  def singleton = BTree(1)
  def pair = BTree(2, 1)

  def beEmpty = empty.isEmpty must beTrue
  def haveLength0 = empty.size must_== 0

  def singletonNotEmpty = singleton.nonEmpty must beTrue
  def singletonSize1 = singleton.size must_== 1
  def singletonContains1 = singleton.contains(1) must beTrue

  def pairContainsElementsInOrder = pair.toVector must beEqualTo(Vector(1, 2))

  def containsAllInsertedElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    val tree = BTree(elements: _*)
    elements.forall(tree.contains)
  }
  def containAllDistinctElements = Prop.forAll(arbitrary[List[Int]]) { elements =>
    BTree(elements: _*).size must_== elements.toVector.distinct.size
  }
  def containsElementsInOrder = Prop.forAll(arbitrary[List[Int]]) { elements =>
    BTree(elements: _*).toVector must_== elements.toVector.sorted.distinct
  }

  def example(elts: Int*) = BTree(elts: _*).toVector must_== Vector(elts: _*).distinct.sorted
}
