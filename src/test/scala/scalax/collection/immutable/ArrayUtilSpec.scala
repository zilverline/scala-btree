package scalax.collection.immutable

import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop
import org.scalacheck.Arbitrary
import scala.reflect.ClassTag

class ArrayUtilSpec extends org.specs2.Specification with org.specs2.ScalaCheck {
  import implementation.insertValue

  def is = s2"""
Array utilities

Partially copy an array and insert a single value
  produce array with increased length               $insertValueIncreasesLength
  keep all elements before insertion point the same $insertValueKeepLowerElementsSame
  shift all elements after insertion point higher   $insertValueShiftAfterInsertionPoint
  insert value at insertion point                   $insertValueIsInserted
  """

  implicit def ArbitraryInteger: Arbitrary[java.lang.Integer] = Arbitrary(for {
    i <- arbitrary[Int]
  } yield java.lang.Integer.valueOf(i))

  def ArbitraryListAndInsertionValue[A: Arbitrary]: Gen[(List[A], Int, Int, Int, A)] = for {
    list <- arbitrary[List[A]]
    start <- Gen.chooseNum(0, list.length)
    end <- Gen.chooseNum(start, list.length)
    insertionPoint <- Gen.chooseNum(start, end)
    value <- arbitrary[A]
  } yield {
    (list, start, end, insertionPoint, value)
  }
  def insertValueIncreasesLength = Prop.forAllNoShrink(ArbitraryListAndInsertionValue[java.lang.Integer]) {
    case (list, start, end, insertionPoint, value) =>
      val array = list.toArray[AnyRef]
      insertValue(array, start, end, insertionPoint, value).length must_== (end - start + 1)
  }
  def insertValueKeepLowerElementsSame = Prop.forAllNoShrink(ArbitraryListAndInsertionValue[java.lang.Integer]) {
    case (list, start, end, insertionPoint, value) =>
      val array = list.toArray[AnyRef]
      val inserted = insertValue(array, start, end, insertionPoint, value)
      (start until insertionPoint).forall { i => array(i) aka s"element $i" must_== inserted(i - start) }
  }
  def insertValueShiftAfterInsertionPoint = Prop.forAllNoShrink(ArbitraryListAndInsertionValue[java.lang.Integer]) {
    case (list, start, end, insertionPoint, value) =>
      val array = list.toArray[AnyRef]
      val inserted = insertValue(array, start, end, insertionPoint, value)
      (insertionPoint until end).forall { i => array(i) aka s"element $i" must_== inserted(i + 1 - start) }
  }
  def insertValueIsInserted = Prop.forAllNoShrink(ArbitraryListAndInsertionValue[java.lang.Integer]) {
    case (list, start, end, insertionPoint, value) =>
      val array = list.toArray[AnyRef]
      val inserted = insertValue(array, start, end, insertionPoint, value)
      inserted(insertionPoint - start) aka "inserted value" must_== value
  }
}
