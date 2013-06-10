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

  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  type Node[L, A] = Array[AnyRef] @@ (L, A)

  class NodeBuilder[L, A] {
    private var node: Node[L, A] = _
    private var index: Int = _

    final def result(): Node[L, A] = {
      val r = node
      node = null
      index = -1
      r
    }
    final def allocLeaf(n: Int)(implicit ev: L =:= Leaf) = {
      node = new Array[AnyRef](n).asInstanceOf[Node[L, A]]
      index = 0
      node
    }
    final def allocInternal(n: Int)(implicit ev: L <:< Next[_]) = {
      node = new Array[AnyRef](1 + n + n + 1).asInstanceOf[Node[L, A]]
      index = 0
      node
    }
    final def setSize(size: Int)(implicit ev: L <:< Next[_]) = node(0) = size.asInstanceOf[AnyRef]

    final def copy(source: Node[L, A], from: Int, to: Int): Unit = {
      //    assert(target >= 0, s"$target >= 0")
      //    assert(length >= 0, s"$length >= 0")
      //    assert(target + length <= dest.length, s"$target + $length <= ${dest.length}")
      var i = 0
      while (i < to - from) {
        node(index + i) = source(from + i)
        i += 1
      }
      index += to - from
    }
    @inline final def insertValue(v: A): Unit = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
    }
    @inline final def insertChild[M](v: Node[M, A])(implicit ev: L =:= Next[M]): Unit = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
    }
  }

  object Node {
    def leaf[A: Ordering](elements: Array[A]): Node[Leaf, A] = elements.asInstanceOf[Node[Leaf, A]]
    def nonLeaf[L, A: Ordering](left: Node[L, A], middle: A, right: Node[L, A])(implicit childOps: NodeOps[L, A]): Node[Next[L], A] = {
      val array = new Array[AnyRef](4).asInstanceOf[Node[Next[L], A]]
      array(0) = (1 + childOps.size(left) + childOps.size(right)).asInstanceOf[AnyRef]
      array(1) = middle.asInstanceOf[AnyRef]
      array(2) = left
      array(3) = right
      array
    }
  }

  sealed trait NodeOps[L, A] {
    type N = Node[L, A]

    def size(node: N): Int

    def contains(node: N, a: A): Boolean
    def insert(node: N, a: A)(implicit builder: NodeBuilder[L, A]): Either[N, (N, A, N)]

    def toVector(node: N): Vector[A] = {
      val builder = Vector.newBuilder[A]
      buildCollection(builder, node)
      builder.result
    }

    def buildCollection(builder: Builder[A, _], node: N): Unit
  }

  implicit def castingOrdering[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = ordering.asInstanceOf[Comparator[AnyRef]]

  implicit def LeafOps[A: Ordering] = new NodeOps[Leaf, A] {
    val order = 32
    val halfOrder = order / 2

    override def size(node: N): Int = node.length
    override def contains(node: N, a: A): Boolean = search(node, a) >= 0
    override def buildCollection(builder: Builder[A, _], node: N): Unit = {
      builder ++= node.asInstanceOf[Array[A]]
    }

    override def insert(node: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Either[N, (N, A, N)] = {
      val index = search(node, a)
      if (index >= 0) {
        Left(copyAndUpdateValue(node, 0, node.length, index, a))
      } else {
        val insertionPoint = -index - 1
        if (size(node) < order) {
          Left(copyAndInsertValue(node, 0, size(node), insertionPoint, a))
        } else {
          Right(if (insertionPoint < halfOrder) {
            val left: N = copyAndInsertValue(node, 0, halfOrder - 1, insertionPoint, a)
            val middle: A = node(halfOrder - 1).asInstanceOf[A]
            val right: N = copyOfRange(node, halfOrder, order)
            (left, middle, right)
          } else if (insertionPoint > halfOrder) {
            val left: N = copyOfRange(node, 0, halfOrder)
            val middle: A = node(halfOrder).asInstanceOf[A]
            val right: N = copyAndInsertValue(node, halfOrder + 1, order, insertionPoint, a)
            (left, middle, right)
          } else {
            val left: N = copyOfRange(node, 0, halfOrder)
            val middle: A = a
            val right: N = copyOfRange(node, halfOrder, order)
            (left, middle, right)
          })
        }
      }
    }

    override def toVector(node: N): Vector[A] = node.asInstanceOf[Array[A]].toVector

    @inline private def search(node: N, a: A): Int = Arrays.binarySearch(node, a.asInstanceOf[AnyRef], castingOrdering[A])
  }

  @inline private def copyOfRange[L, A](source: Node[L, A], from: Int, to: Int): Node[L, A] = Arrays.copyOfRange(source, from, to).asInstanceOf[Node[L, A]]
  @inline private def copyAndUpdateValue[L, A](node: Node[L, A], from: Int, to: Int, index: Int, value: A): Node[L, A] = {
    val result = copyOfRange(node, from, to)
    result(index - from) = value.asInstanceOf[AnyRef]
    result
  }
  @inline private[immutable] def copyAndInsertValue[L, A](source: Node[L, A], from: Int, to: Int, position: Int, value: A): Node[L, A] = {
    //    assert(end >= start, "end >= start")
    //    assert(start <= position, "start <= position")
    //    assert(position <= end, "position <= end")
    val result = new Array[AnyRef](to - from + 1).asInstanceOf[Node[L, A]]
    val before = position - from
    var i = copy(result, 0, source, from, before)
    i = insertValue(result, i, value)
    i = copy(result, i, source, position, to - position)
    result
  }

  implicit def NextOps[L, A: Ordering](implicit childOps: NodeOps[L, A]): NodeOps[Next[L], A] = new NodeOps[Next[L], A] {
    type ChildNode = childOps.N

    val order = 16
    val halfOrder = order / 2

    private def childCount(node: N): Int = (node.length - 2) / 2
    override def size(node: N): Int = node(0).asInstanceOf[Int]
    override def contains(node: N, a: A): Boolean = {
      val children = childCount(node)
      val index = Arrays.binarySearch(node, 1, children + 1, a.asInstanceOf[AnyRef], castingOrdering[A])
      index >= 0 || {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node(childIndex).asInstanceOf[childOps.N]
        childOps.contains(child, a)
      }
    }

    private def print_(node: N): Unit = {
      val indices = (0 until node.length).map(_.formatted("%5d")).mkString(", ")
      val elts = node.drop(1).take(childCount(node)).map(_.formatted("%5d"))
      val children = node.takeRight(childCount(node) + 1).map { _.asInstanceOf[Node[_, _]].mkString("<", ",", ">") }
      val contents = Seq(f"${size(node)}%5d") ++ elts ++ children mkString (", ")
      //println(s"$indices\n$contents")
    }

    private def updateSize(node: N): N = {
      val children = childCount(node)
      var i = children + 1
      var size = children
      while (i < node.length) {
        size += childOps.size(node(i).asInstanceOf[Node[L, A]])
        i += 1
      }
      node(0) = size.asInstanceOf[AnyRef]
      node
    }
    override def insert(node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Either[N, (N, A, N)] = {
      val elt = a.asInstanceOf[AnyRef]
      val children = childCount(node)
      val index = Arrays.binarySearch(node, 1, children + 1, elt, castingOrdering[A])
      if (index >= 0) {
        Left(copyAndUpdateValue(node, 0, node.length, index, a))
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node(childIndex).asInstanceOf[childOps.N]
        childOps.insert(child, a)(builder.asInstanceOf[NodeBuilder[L, A]]) match {
          case Left(updatedChild) =>
            val updated = copyOfRange(node, 0, node.length)
            updated(childIndex) = updatedChild
            val sizeChange = childOps.size(updatedChild) - childOps.size(child)
            updated(0) = (updated(0).asInstanceOf[Int] + sizeChange).asInstanceOf[AnyRef]
            Left(updated)
          case Right((left, middle, right)) =>
            if (children < order) {
//              builder.allocInternal(children + 1)
//              builder.copy(node, 0, insertionPoint)
//              builder.insertValue(middle)
//              builder.copy(node, insertionPoint, childIndex)
//              builder.insertChild(left)
//              builder.insertChild(right)
//              builder.copy(node, childIndex + 1, node.length)
//              builder.setSize(node(0).asInstanceOf[Int] + 1)
//              Left(builder.result())
              val updated = new Array[AnyRef](node.length + 2).asInstanceOf[N]
              var i = copy(updated, 0, node, 0, insertionPoint)
              i = insertValue(updated, i, middle)
              i = copy(updated, i, node, insertionPoint, childIndex - insertionPoint)
              i = insertChild(updated, i, left)
              i = insertChild(updated, i, right)
              i = copy(updated, i, node, childIndex + 1, node.length - childIndex - 1)
              updated(0) = (updated(0).asInstanceOf[Int] + 1).asInstanceOf[AnyRef]
              Left(updated)
            } else {
              Right(if (insertionPoint < halfOrder + 1) {
                val l: N = {
                  val result = allocSplitNode
                  // Keys
                  val before = insertionPoint - 1
                  var i = copy(result, 1, node, 1, before)
                  i = insertValue(result, i, middle)
                  i = copy(result, i, node, insertionPoint, halfOrder - before - 1)
                  // Children
                  i = copy(result, i, node, children + 1, before)
                  i = insertChild(result, i, left)
                  i = insertChild(result, i, right)
                  i = copy(result, i, node, childIndex + 1, halfOrder - before - 1)
                  updateSize(result)
                }
                val m: A = node(halfOrder).asInstanceOf[A]
                val r: N = {
                  val result = allocSplitNode
                  var i = copy(result, 1, node, 1 + halfOrder, halfOrder)
                  i = copy(result, i, node, 1 + order + halfOrder, halfOrder + 1)
                  updateSize(result)
                }
                (l, m, r)
              } else if (insertionPoint > halfOrder + 1) {
                val l: N = {
                  val result = allocSplitNode
                  var i = copy(result, 1, node, 1, halfOrder)
                  i = copy(result, i, node, 1 + order, halfOrder + 1)
                  updateSize(result)
                }
                val m: A = node(halfOrder + 1).asInstanceOf[A]
                val r: N = {
                  val result = allocSplitNode
                  // Keys
                  val before = insertionPoint - 1 - halfOrder
                  var i = copy(result, 1, node, halfOrder + 2, before - 1)
                  i = insertValue(result, i, middle)
                  i = copy(result, i, node, insertionPoint, halfOrder - before)
                  // Children
                  i = copy(result, i, node, children + halfOrder + 2, before - 1)
                  i = insertChild(result, i, left)
                  i = insertChild(result, i, right)
                  i = copy(result, i, node, childIndex + 1, halfOrder - before)
                  updateSize(result)
                }
                (l, m, r)
              } else {
                val l: N = {
                  val result = allocSplitNode
                  var i = copy(result, 1, node, 1, halfOrder)
                  i = copy(result, i, node, 1 + order, halfOrder)
                  result(i) = left
                  updateSize(result)
                }
                val m: A = middle
                val r: N = {
                  val result = allocSplitNode
                  var i = copy(result, 1, node, 1 + halfOrder, halfOrder)
                  i = insertChild(result, i, right)
                  i = copy(result, i, node, 1 + order + halfOrder + 1, halfOrder)
                  updateSize(result)
                }
                (l, m, r)
              })
            }
        }
      }

    }

    @inline private def allocSplitNode: N = new Array[AnyRef](1 + halfOrder + halfOrder + 1).asInstanceOf[N]

    override def buildCollection(builder: Builder[A, _], node: N): Unit = {
      val children = childCount(node)
      var i = 0
      while (i < children) {
        val childNode = node(i + children + 1).asInstanceOf[ChildNode]
        childOps.buildCollection(builder, childNode)
        builder += node(i + 1).asInstanceOf[A]
        i += 1
      }
      childOps.buildCollection(builder, node.last.asInstanceOf[ChildNode])
    }
  }

  class Root[L, A: Ordering](val root: Node[L, A])(implicit val ops: NodeOps[L, A]) extends BTree[A] {
    implicit def newBuilder: NodeBuilder[L, A] = new NodeBuilder[L, A]()
    override def +(a: A) = ops.insert(root, a) match {
      case Left(node)                   => new Root[L, A](node)
      case Right((left, middle, right)) => new Root[Next[L], A](Node.nonLeaf(left, middle, right))
    }
    override def isEmpty: Boolean = size == 0
    override def nonEmpty: Boolean = !isEmpty
    override def size: Int = ops.size(root)
    override def contains(a: A): Boolean = ops.contains(root, a)
    override def toVector: Vector[A] = ops.toVector(root)
  }

  private def copy[L, A](dest: Node[L, A], target: Int, source: Node[L, A], start: Int, length: Int): Int = {
    //    assert(target >= 0, s"$target >= 0")
    //    assert(length >= 0, s"$length >= 0")
    //    assert(target + length <= dest.length, s"$target + $length <= ${dest.length}")
    var i = 0
    while (i < length) {
      dest(target + i) = source(start + i)
      i += 1
    }
    target + i
  }
  @inline private def insertValue[L, A](dest: Node[L, A], i: Int, v: A): Int = {
    //    assert(i >= 0, s"$i >= 0")
    //    assert(i < dest.length, s"$i < ${dest.length}")
    dest(i) = v.asInstanceOf[AnyRef]
    i + 1
  }
  @inline private def insertChild[L, A](dest: Node[Next[L], A], i: Int, v: Node[L, A]): Int = {
    //    assert(i >= 0, s"$i >= 0")
    //    assert(i < dest.length, s"$i < ${dest.length}")
    dest(i) = v.asInstanceOf[AnyRef]
    i + 1
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
  def empty[A: Ordering]: BTree[A] = new Root[Leaf, A](Array.empty[AnyRef].asInstanceOf[Node[Leaf, A]])
}
