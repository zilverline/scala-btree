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
    private var children: Int = _

    final def result(): Node[L, A] = {
      val r = node
      node = null
      index = -1
      r
    }
    final def allocLeaf(n: Int)(implicit ev: L =:= Leaf) = {
      node = new Array[AnyRef](n).asInstanceOf[Node[L, A]]
      index = 0
      children = n
      node
    }
    final def allocInternal(n: Int)(implicit ev: L <:< Next[_]) = {
      node = new Array[AnyRef](1 + n + n + 1).asInstanceOf[Node[L, A]]
      index = 1
      children = n
      node
    }
    final def allocCopy(n: Node[L, A]): Unit = {
      node = Arrays.copyOf(n, n.length).asInstanceOf[Node[L, A]]
    }
    final def setSize(size: Int)(implicit ev: L <:< Next[_]): Unit = node(0) = size.asInstanceOf[AnyRef]
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
    final def insertValue(v: A): Unit = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
    }
    final def insertChild[M](v: Node[M, A])(implicit ev: L =:= Next[M]): Unit = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
    }
    final def updateValue(index: Int, value: A): Unit = {
      node(index) = value.asInstanceOf[AnyRef]
    }
    final def updateChild[M](index: Int, child: Node[M, A])(implicit ev: L =:= Next[M]): Unit = {
      node(index) = child.asInstanceOf[AnyRef]
    }
  }

  object Node {
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
        builder.allocCopy(node)
        builder.updateValue(index, a)
        Left(builder.result())
      } else {
        val insertionPoint = -index - 1
        if (size(node) < order) {
          builder.allocLeaf(node.length + 1)
          builder.copy(node, 0, insertionPoint)
          builder.insertValue(a)
          builder.copy(node, insertionPoint, node.length)
          Left(builder.result())
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

    private def copyOfRange(source: N, from: Int, to: Int): N = Arrays.copyOfRange(source, from, to).asInstanceOf[N]
    private def copyAndInsertValue(source: N, from: Int, to: Int, position: Int, value: A)(implicit builder: NodeBuilder[Leaf, A]): N = {
      builder.allocLeaf(to - from + 1)
      builder.copy(source, from, position)
      builder.insertValue(value)
      builder.copy(source, position, to)
      builder.result()
    }

    override def toVector(node: N): Vector[A] = node.asInstanceOf[Array[A]].toVector

    @inline private def search(node: N, a: A): Int = Arrays.binarySearch(node, a.asInstanceOf[AnyRef], castingOrdering[A])
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
      val children = childCount(node)
      val index = Arrays.binarySearch(node, 1, children + 1, a.asInstanceOf[AnyRef], castingOrdering[A])
      if (index >= 0) {
        builder.allocCopy(node)
        builder.updateValue(index, a)
        Left(builder.result())
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node(childIndex).asInstanceOf[childOps.N]
        childOps.insert(child, a)(builder.asInstanceOf[NodeBuilder[L, A]]) match {
          case Left(updatedChild) =>
            builder.allocCopy(node)
            builder.updateChild(childIndex, updatedChild)
            val sizeChange = childOps.size(updatedChild) - childOps.size(child)
            builder.setSize(size(node) + sizeChange)
            Left(builder.result())
          case Right((left, middle, right)) =>
            if (children < order) {
              builder.allocInternal(children + 1)
              builder.setSize(size(node) + 1)
              builder.copy(node, 1, insertionPoint)
              builder.insertValue(middle)
              builder.copy(node, insertionPoint, childIndex)
              builder.insertChild(left)
              builder.insertChild(right)
              builder.copy(node, childIndex + 1, node.length)
              Left(builder.result())
            } else {
              Right(if (insertionPoint < halfOrder + 1) {
                val l: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1, insertionPoint)
                  builder.insertValue(middle)
                  builder.copy(node, insertionPoint, 1 + halfOrder - 1)
                  builder.copy(node, 1 + children, childIndex)
                  builder.insertChild(left)
                  builder.insertChild(right)
                  builder.copy(node, childIndex + 1, 1 + order + halfOrder)
                  updateSize(builder.result())
                }
                val m: A = node(halfOrder).asInstanceOf[A]
                val r: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1 + halfOrder, 1 + children)
                  builder.copy(node, 1 + children + halfOrder, node.length)
                  updateSize(builder.result())
                }
                (l, m, r)
              } else if (insertionPoint > halfOrder + 1) {
                val l: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1, 1 + halfOrder)
                  builder.copy(node, 1 + children, 1 + children + halfOrder + 1)
                  updateSize(builder.result())
                }
                val m: A = node(halfOrder + 1).asInstanceOf[A]
                val r: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1 + halfOrder + 1, insertionPoint)
                  builder.insertValue(middle)
                  builder.copy(node, insertionPoint, 1 + children)
                  builder.copy(node, 1 + children + halfOrder + 1, childIndex)
                  builder.insertChild(left)
                  builder.insertChild(right)
                  builder.copy(node, childIndex + 1, node.length)
                  updateSize(builder.result())
                }
                (l, m, r)
              } else {
                val l: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1, 1 + halfOrder)
                  builder.copy(node, 1 + order, 1 + order + halfOrder)
                  builder.insertChild(left)
                  updateSize(builder.result())
                }
                val m: A = middle
                val r: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1 + halfOrder, 1 + order)
                  builder.insertChild(right)
                  builder.copy(node, 1 + order + halfOrder + 1, node.length)
                  updateSize(builder.result())
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
