package scalax.collection.immutable

import scala.reflect.ClassTag
import scala.language.higherKinds
import java.util.Arrays
import java.util.Comparator
import scala.collection.mutable.Builder

private[immutable] object implementation {
  sealed trait Level
  final class Leaf extends Level
  final class Next[L] extends Level

  sealed trait NodeOps[L, A] {
    type N = Node[L, A]

    val order = 6
    val halfOrder = order / 2

    def size(node: N): Int

    def contains(node: N, a: A): Boolean
    def insert(node: N, a: A): Either[N, (N, A, N)]

    def toVector(node: N): Vector[A] = {
      val builder = Vector.newBuilder[A]
      buildCollection(builder, node)
      builder.result
    }

    def buildCollection(builder: Builder[A, _], node: N): Unit
  }

  private def copy(dest: Array[AnyRef], target: Int, source: Array[AnyRef], start: Int, length: Int): Int = {
    assert(target >= 0, s"$target >= 0")
    //assert(length >= 0, s"$length >= 0")
    assert(target + length <= dest.length, s"$target + $length <= ${dest.length}")
    var i = 0
    while (i < length) {
      dest(target + i) = source(start + i)
      i += 1
    }
    target + i
  }
  private def ins(dest: Array[AnyRef], i: Int, v: AnyRef): Int = {
    assert(i >= 0, s"$i >= 0")
    assert(i < dest.length, s"$i < ${dest.length}")
    dest(i) = v
    i + 1
  }

  private[immutable] def insertValue(source: Array[AnyRef], start: Int, end: Int, position: Int, value: AnyRef): Array[AnyRef] = {
    assert(end >= start, "end >= start")
    assert(start <= position, "start <= position")
    assert(position <= end, "position <= end")
    val result = new Array[AnyRef](end - start + 1)
    val before = position - start
    var i = copy(result, 0, source, start, before)
    i = ins(result, i, value)
    i = copy(result, i, source, position, end - position)
    result
  }
  private[immutable] def iiu(source: Array[AnyRef], start: Int, end: Int, ii1: Int, iv1: AnyRef, ii2: Int, iv2: AnyRef, ui: Int, uv: AnyRef): Array[AnyRef] = {
    val result = new Array[AnyRef](end - start + 2)
    var i = copy(result, 0, source, start, ii1 - start)
    i = ins(result, i, iv1)
    i = copy(result, i, source, ii1, ii2 - ii1)
    i = ins(result, i, iv2)
    i = copy(result, i, source, ii2, ui - ii2)
    i = ins(result, i, uv)
    i = copy(result, i, source, ui + 1, end - ui - 1)
    result
  }

  implicit def castingOrdering[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = new Comparator[AnyRef] {
    override def compare(x: AnyRef, y: AnyRef): Int = ordering.compare(x.asInstanceOf[A], y.asInstanceOf[A])
  }

  implicit def LeafOps[A: Ordering] = new NodeOps[Leaf, A] {
    override def size(node: N): Int = node.elements.length
    override def contains(node: N, a: A): Boolean = node.elements.contains(a)
    override def buildCollection(builder: Builder[A, _], node: N): Unit = {
      builder ++= node.elements.asInstanceOf[Array[A]]
    }

    override def insert(node: N, a: A): Either[N, (N, A, N)] = {
      val elt = a.asInstanceOf[AnyRef]
      val index = Arrays.binarySearch(node.elements, elt, castingOrdering[A])
      if (index >= 0) {
        val updated = Arrays.copyOf(node.elements, node.elements.length)
        updated(index) = elt
        Left(new Node(updated))
      } else {
        val insertionPoint = -index - 1
        if (size(node) < order) {
          Left(new Node(insertValue(node.elements, 0, size(node), insertionPoint, elt)))
        } else {
          Right(if (insertionPoint < halfOrder) {
            val left: N = new Node(insertValue(node.elements, 0, halfOrder - 1, insertionPoint, elt))
            val middle: A = node.elements(halfOrder - 1).asInstanceOf[A]
            val right: N = new Node(Arrays.copyOfRange(node.elements, halfOrder, order))
            (left, middle, right)
          } else if (insertionPoint > halfOrder) {
            val left: N = new Node(Arrays.copyOfRange(node.elements, 0, halfOrder))
            val middle: A = node.elements(halfOrder).asInstanceOf[A]
            val right: N = new Node(insertValue(node.elements, halfOrder + 1, order, insertionPoint, elt))
            (left, middle, right)
          } else {
            val left: N = new Node(Arrays.copyOfRange(node.elements, 0, halfOrder))
            val middle: A = a
            val right: N = new Node(Arrays.copyOfRange(node.elements, halfOrder, order))
            (left, middle, right)
          })
        }
      }
    }

    override def toVector(node: N): Vector[A] = node.elements.asInstanceOf[Array[A]].toVector
  }

  private implicit class DebugOps[A](a: A) {
    def pp = { /*println(s"$a");*/ a }
    def pp(msg: String) = { /*println(s"$msg: $a");*/ a }
  }
  implicit def NextOps[L, A: Ordering](implicit childOps: NodeOps[L, A]): NodeOps[Next[L], A] = new NodeOps[Next[L], A] {
    type ChildNode = childOps.N

    private def childCount(node: N): Int = (node.elements.length - 2) / 2
    override def size(node: N): Int = node.elements(0).asInstanceOf[Int]
    override def contains(node: N, a: A): Boolean = node.elements.contains(a)

    private def print(node: N): Unit = {
      val indices  = (0 until node.elements.length).map(_.formatted("%5d")).mkString(", ")
      val elts = node.elements.drop(1).take(childCount(node)).map(_.formatted("%5d"))
      val children = node.elements.takeRight(childCount(node) + 1).map { _.asInstanceOf[Node[_, _]].elements.mkString("<", ",", ">") }
      val contents = Seq(f"${size(node)}%5d") ++ elts ++ children mkString (", ")
      //println(s"$indices\n$contents")
    }

    private def updateSize(node: N): N = {
      val children = childCount(node)
      var i = children + 1
      var size = children
      while (i < node.elements.length) {
        size += childOps.size(node.elements(i).asInstanceOf[Node[L, A]])
        i += 1
      }
      node.elements(0) = size.asInstanceOf[AnyRef]
      node
    }
    override def insert(node: N, a: A): Either[N, (N, A, N)] = {
      val elt = a.asInstanceOf[AnyRef]
      val children = childCount(node)
      val index = Arrays.binarySearch(node.elements, 1, children + 1, elt, castingOrdering[A])
      if (index >= 0) {
        val updated = Arrays.copyOf(node.elements, node.elements.length)
        updated(index) = elt
        Left(new Node(updated))
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node.elements(childIndex).asInstanceOf[childOps.N]
        childOps.insert(child, a) match {
          case Left(updatedChild) =>
            val updated = Arrays.copyOf(node.elements, node.elements.length)
            updated(insertionPoint + children) = updatedChild
            Left(new Node(updated))
          case Right((left, middle, right)) =>
            if (children < order) {
              val updated = iiu(node.elements, 0, node.elements.length, insertionPoint, middle.asInstanceOf[AnyRef], childIndex, left, childIndex, right)
              updated(0) = (updated(0).asInstanceOf[Int] + 1).asInstanceOf[AnyRef]
              Left(new Node(updated))
            } else {
              Right(if (insertionPoint < halfOrder + 1) {
                val l: N = {
                  middle.pp(s"Splitting left node for key at $insertionPoint")
                  print(node)
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  // Keys
                  val before = insertionPoint - 1
                  require(before >= 0, s"$before >= 0")
                  var i = copy(result, 1, node.elements, 1.pp(s"keys less (#${before})"), before)
                  i = ins(result, i, middle.asInstanceOf[AnyRef])
                  i = copy(result, i, node.elements, insertionPoint.pp(s"keys greater (#${halfOrder - before})"), halfOrder - before - 1)
                  // Children
                  i = copy(result, i, node.elements, children + 1 pp "children less", before)
                  i = ins(result, i, left)
                  i = ins(result, i, right)
                  i = copy(result, i, node.elements, (childIndex + 1).pp("children greater"), halfOrder - before - 1)
                  require(i == result.length, s"$i == ${result.length}")
                  val n = updateSize(new Node(result))
                  print(n)
                  n
                }
                val m: A = node.elements(halfOrder).asInstanceOf[A]
                val r: N = {
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  var i = copy(result, 1, node.elements, 1 + halfOrder, halfOrder)
                  i = copy(result, i, node.elements, 1 + order + halfOrder, halfOrder + 1)
                  updateSize(new Node(result))
                }
                (l, m, r)
              } else if (insertionPoint > halfOrder + 1) {
                val l: N = {
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  var i = copy(result, 1, node.elements, 1, halfOrder)
                  i = copy(result, i, node.elements, 1 + order, halfOrder + 1)
                  updateSize(new Node(result))
                }
                val m: A = node.elements(halfOrder + 1).asInstanceOf[A]
                val r: N = {
                  middle.pp(s"Splitting right node for key at $insertionPoint")
                  print(node)
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  // Keys
                  val before = insertionPoint - 1 - halfOrder
                  require(before.pp("before") > 0, s"$before > 0")
                  var i = copy(result, 1, node.elements, (halfOrder + 2).pp(s"keys less (#${before})"), before - 1)
                  i = ins(result, i, middle.asInstanceOf[AnyRef])
                  i = copy(result, i, node.elements, insertionPoint.pp(s"keys greater (#${halfOrder - before})"), halfOrder - before)
                  // Children
                  i = copy(result, i, node.elements, (children + halfOrder + 2).pp("children less"), before - 1)
                  i = ins(result, i, left)
                  i = ins(result, i, right)
                  i = copy(result, i, node.elements, (childIndex + 1).pp("children greater"), halfOrder - before)
                  require(i == result.length, s"$i == ${result.length}")
                  val n = updateSize(new Node(result))
                  print(n)
                  n
                }
                (l, m, r)
              } else {
                middle.pp(s"Splitting in the middle at $insertionPoint ($childIndex)")
                print(node)
                val l: N = {
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  var i = copy(result, 1, node.elements, 1, halfOrder)
                  i = copy(result, i, node.elements, 1 + order, halfOrder)
                  result(i) = left
                  val n = updateSize(new Node(result))
                  print(n)
                  n
                }
                val m: A = middle
                val r: N = {
                  val result = new Array[AnyRef](1 + halfOrder + halfOrder + 1)
                  var i = copy(result, 1, node.elements, 1 + halfOrder, halfOrder)
                  i = ins(result, i, right)
                  i = copy(result, i, node.elements, 1 + order + halfOrder + 1, halfOrder)
                  val n = updateSize(new Node(result))
                  print(n)
                  n
                }
                (l, m, r)
              })
            }
        }
      }
    }

    override def buildCollection(builder: Builder[A, _], node: N): Unit = {
      val children = childCount(node)
      var i = 0
      while (i < children) {
        val childNode = node.elements(i + children + 1).asInstanceOf[ChildNode]
        childOps.buildCollection(builder, childNode)
        builder += node.elements(i + 1).asInstanceOf[A]
        i += 1
      }
      childOps.buildCollection(builder, node.elements.last.asInstanceOf[ChildNode])
    }
  }

  class Node[L, A](val elements: Array[AnyRef]) {
    def size(implicit ops: NodeOps[L, A]) = ops.size(this)
  }

  object Node {
    def leaf[A: Ordering](elements: Array[A]): Node[Leaf, A] = new Node[Leaf, A](elements.asInstanceOf[Array[AnyRef]])
    def nonLeaf[L, A: Ordering](left: Node[L, A], middle: A, right: Node[L, A])(implicit childOps: NodeOps[L, A]): Node[Next[L], A] = {
      val array = new Array[AnyRef](4)
      array(0) = (1 + left.size + right.size).asInstanceOf[AnyRef]
      array(1) = middle.asInstanceOf[AnyRef]
      array(2) = left
      array(3) = right
      new Node[Next[L], A](array)
    }
  }

  class Root[L, A: Ordering](val root: Node[L, A])(implicit val ops: NodeOps[L, A]) extends BTree[A] {
    override def +(a: A) = ops.insert(root, a) match {
      case Left(node)                   => new Root[L, A](node)
      case Right((left, middle, right)) => new Root(Node.nonLeaf(left, middle, right))
    }
    override def isEmpty: Boolean = size == 0
    override def nonEmpty: Boolean = !isEmpty
    override def size: Int = ops.size(root)
    override def contains(a: A): Boolean = ops.contains(root, a)
    override def toVector: Vector[A] = ops.toVector(root)
  }
}

trait BTree[A] {
  def +(a: A): BTree[A]
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty
  def size: Int
  def contains(a: A): Boolean
  def toVector: Vector[A]
}
object BTree {
  import implementation._

  def apply[A: Ordering](elements: A*): BTree[A] = elements.foldLeft(BTree.empty[A])(_ + _)
  def empty[A: Ordering]: BTree[A] = new Root[Leaf, A](new Node(Array.empty[AnyRef]))
}
