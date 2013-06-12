package scalax.collection.immutable

import scala.reflect.ClassTag
import scala.language.higherKinds
import java.util.Arrays
import java.util.Comparator
import scala.collection.mutable.Builder

private[immutable] object implementation {
  private def TODO = throw new RuntimeException("TODO")

  implicit class Debug[A](o: A) {
    def pp: A = { println(o); o }
  }
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

    private def invariant() = {
      assert(node.forall(_ ne null), s"contains null: ${node.mkString("(", ",", ")")}")
    }

    final def result(): Node[L, A] = {
      invariant()
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
    final def recalculateSize()(implicit ev: L <:< Next[_], ops: NodeOps[L, A]): Unit = {
      val children = ops.valueCount(node)
      var size = children
      var i = 1 + children
      while (i < node.length) {
        size += ops.myChildOps.size(ops.child(node, i))
        i += 1
      }
      node(0) = size.asInstanceOf[AnyRef]
    }
    final def updateValue(index: Int, value: A): Unit = {
      node(index) = value.asInstanceOf[AnyRef]
    }
    final def updateChild[M](index: Int, child: Node[M, A])(implicit ev: L =:= Next[M]): Unit = {
      node(index) = child.asInstanceOf[AnyRef]
    }

    final def down[M](implicit ev: L =:= Next[M]): NodeBuilder[M, A] = this.asInstanceOf[NodeBuilder[M, A]]

    final def nonLeaf(left: Node[L, A], middle: A, right: Node[L, A])(implicit childOps: NodeOps[L, A]): Node[Next[L], A] = {
      val array = new Array[AnyRef](4).asInstanceOf[Node[Next[L], A]]
      array(0) = (1 + childOps.size(left) + childOps.size(right)).asInstanceOf[AnyRef]
      array(1) = middle.asInstanceOf[AnyRef]
      array(2) = left
      array(3) = right
      array
    }
  }

  object Node {
  }

  sealed trait NodeOps[L, A] {
    type N = Node[L, A]

    type Level = L
    type NextLevel = Next[L]
    type PreviousLevel

    type ChildNode = Node[PreviousLevel, A]
    type ChildOps = NodeOps[PreviousLevel, A]

    def newBuilder = new NodeBuilder[L, A]

    def valueCount(node: N): Int

    def myChildOps: ChildOps

    def child(node: N, index: Int): ChildNode
    def size(node: N): Int

    def contains(node: N, a: A): Boolean
    def insert(node: N, a: A)(implicit builder: NodeBuilder[L, A]): Either[N, (N, A, N)]

    def deleteFromRoot(node: N, a: A)(implicit builder: NodeBuilder[L, A]): Option[Either[N, ChildNode]]
    def rebalance(left: N, right: N)(implicit builder: NodeBuilder[L, A]): Either[N, (N, A, N)]
    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: NodeBuilder[L, A]): Option[Either[N, (N, A, N)]]
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: NodeBuilder[L, A]): Option[Either[N, (N, A, N)]]

    def toVector(node: N): Vector[A] = {
      val builder = Vector.newBuilder[A]
      buildCollection(builder, node)
      builder.result
    }

    def buildCollection(builder: Builder[A, _], node: N): Unit
  }

  implicit def castingOrdering[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = ordering.asInstanceOf[Comparator[AnyRef]]

  implicit def LeafOps[A: Ordering] = new NodeOps[Leaf, A] {
    override type PreviousLevel = Nothing

    def myChildOps: ChildOps = throw new RuntimeException("no child ops for leaf node")
    def child(node: N, index: Int): ChildNode = throw new RuntimeException("no child ops for leaf node")

    val order = 4
    val halfOrder = order / 2

    def value(node: N, index: Int): A = node(index).asInstanceOf[A]

    override def valueCount(node: N): Int = node.length
    override def size(node: N): Int = valueCount(node)
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

    override def deleteFromRoot(node: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, ChildNode]] = {
      val index = search(node, a)
      if (index < 0)
        None
      else {
        builder.allocLeaf(node.length - 1)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Left(builder.result()))
      }
    }

    override def rebalance(left: N, right: N)(implicit builder: NodeBuilder[Leaf, A]): Either[N, (N, A, N)] = {
      if (left.length == halfOrder && right.length == halfOrder) {
        builder.allocLeaf(left.length + right.length)
        builder.copy(left, 0, left.length)
        builder.copy(right, 0, right.length)
        Left(builder.result())
      } else if (left.length > right.length) {
        builder.allocLeaf(left.length - 1)
        builder.copy(left, 0, left.length - 1)
        val updatedLeft = builder.result()
        val middle = left(left.length - 1).asInstanceOf[A]
        Right((updatedLeft, middle, right))
      } else {
        val middle = right(0).asInstanceOf[A]
        builder.allocLeaf(right.length - 1)
        builder.copy(right, 1, right.length)
        val updatedRight = builder.result()
        Right((left, middle, updatedRight))
      }
    }

    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      val values = valueCount(node)
      if (index < 0)
        None
      else if (values > halfOrder) {
        builder.allocLeaf(values - 1)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Right((leftSibling, leftValue, builder.result())))
      } else if (valueCount(leftSibling) > halfOrder) {
        builder.allocLeaf(leftSibling.length - 1)
        builder.copy(leftSibling, 0, leftSibling.length - 1)
        val updatedLeft = builder.result()
        val updatedMiddle = leftSibling(leftSibling.length - 1).asInstanceOf[A]
        builder.allocLeaf(node.length)
        builder.insertValue(leftValue)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        val updatedRight = builder.result()
        Some(Right((updatedLeft, updatedMiddle, updatedRight)))
      } else {
        builder.allocLeaf(order)
        builder.copy(leftSibling, 0, leftSibling.length)
        builder.insertValue(leftValue)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Left(builder.result()))
      }
    }
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      val values = valueCount(node)
      if (index < 0)
        None
      else if (values > halfOrder) {
        builder.allocLeaf(values - 1)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Right((builder.result(), rightValue, rightSibling)))
      } else if (valueCount(rightSibling) > halfOrder) {
        builder.allocLeaf(node.length)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        builder.insertValue(rightValue)
        val updatedLeft = builder.result()
        val updatedMiddle = value(rightSibling, 0)
        builder.allocLeaf(valueCount(rightSibling) - 1)
        builder.copy(rightSibling, 1, rightSibling.length)
        val updatedRight = builder.result()
        Some(Right((updatedLeft, updatedMiddle, updatedRight)))
      } else {
        builder.allocLeaf(order)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        builder.insertValue(rightValue)
        builder.copy(rightSibling, 0, rightSibling.length)
        Some(Left(builder.result()))
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
    override type ChildNode = childOps.N
    override type PreviousLevel = L

    def myChildOps = childOps

    val order = 4
    val halfOrder = order / 2

    override def valueCount(node: N): Int = (node.length - 2) / 2
    override def size(node: N): Int = {
      assert(node(0) ne null, "size not set")
      node(0).asInstanceOf[Int]
    }
    override def contains(node: N, a: A): Boolean = {
      val children = valueCount(node)
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
      val elts = node.drop(1).take(valueCount(node)).map(_.formatted("%5d"))
      val children = node.takeRight(valueCount(node) + 1).map { _.asInstanceOf[Node[_, _]].mkString("<", ",", ">") }
      val contents = Seq(f"${size(node)}%5d") ++ elts ++ children mkString (", ")
      //println(s"$indices\n$contents")
    }

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, 1, valueCount(node) + 1, a.asInstanceOf[AnyRef], castingOrdering[A])

    override def insert(node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Either[N, (N, A, N)] = {
      val index = search(node, a)
      if (index >= 0) {
        builder.allocCopy(node)
        builder.updateValue(index, a)
        Left(builder.result())
      } else {
        val children = valueCount(node)
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
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = node(halfOrder).asInstanceOf[A]
                val r: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1 + halfOrder, 1 + children)
                  builder.copy(node, 1 + children + halfOrder, node.length)
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              } else if (insertionPoint > halfOrder + 1) {
                val l: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1, 1 + halfOrder)
                  builder.copy(node, 1 + children, 1 + children + halfOrder + 1)
                  builder.recalculateSize()
                  builder.result()
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
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              } else {
                val l: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1, 1 + halfOrder)
                  builder.copy(node, 1 + order, 1 + order + halfOrder)
                  builder.insertChild(left)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = middle
                val r: N = {
                  builder.allocInternal(halfOrder)
                  builder.copy(node, 1 + halfOrder, 1 + order)
                  builder.insertChild(right)
                  builder.copy(node, 1 + order + halfOrder + 1, node.length)
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              })
            }
        }
      }
    }

    override def child(node: N, index: Int) = node(index).asInstanceOf[ChildNode]
    private def leftChild(node: N, index: Int) = child(node, 1 + valueCount(node) + index - 1)
    private def rightChild(node: N, index: Int) = child(node, 1 + valueCount(node) + index)

    override def deleteFromRoot(node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Option[Either[N, ChildNode]] = {
      val index = search(node, a)
      val children = valueCount(node)
      if (index >= 0) {
        val left = leftChild(node, index)
        val right = rightChild(node, index)
        childOps.rebalance(left, right)(builder.asInstanceOf[NodeBuilder[PreviousLevel, A]]) match {
          case Left(updated) if children == 1 =>
            Some(Right(updated))
          case Left(updated) if children > 1 =>
            Some(Left(replaceMergedChildren(node, index, updated)))
          case Right((left, middle, right)) =>
            Some(Left(replaceUpdatedChildren(node, index, left, middle, right)))
        }
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val middleChild = child(node, childIndex)
        val leftSibling = if (childIndex > 1 + children) child(node, childIndex - 1) else null
        val rightSibling = if (childIndex + 1 < node.length) child(node, childIndex + 1) else null
        val leftValue = if (insertionPoint > 1) node(insertionPoint - 1) else null
        val rightValue = if (insertionPoint < 1 + children) node(insertionPoint) else null
        if (leftSibling == null || (rightSibling != null && childOps.valueCount(leftSibling) < childOps.valueCount(rightSibling))) {
          assert(rightValue != null)
          childOps.deleteAndMergeRight(middleChild, rightValue.asInstanceOf[A], rightSibling, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == 1 =>
              Some(Right(merged))
            case Some(Left(merged)) if children > 1 =>
              Some(Left(replaceMergedChildren(node, insertionPoint, merged)))
            case Some(Right((updatedChild, updatedRightValue, updatedRightSibling))) =>
              Some(Left(replaceUpdatedChildren(node, insertionPoint, updatedChild, updatedRightValue, updatedRightSibling)))
          }
        } else {
          childOps.deleteAndMergeLeft(leftSibling, leftValue.asInstanceOf[A], middleChild, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == 1 =>
              Some(Right(merged))
            case Some(Left(merged)) if children > 1 =>
              Some(Left(replaceMergedChildren(node, insertionPoint - 1, merged)))
            case Some(Right((updatedLeftSibling, updatedLeftValue, updatedChild))) =>
              Some(Left(replaceUpdatedChildren(node, insertionPoint - 1, updatedLeftSibling, updatedLeftValue, updatedChild)))
          }
        }
      }
    }
    private def replaceMergedChildren(node: N, insertionPoint: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = insertionPoint + children
      builder.allocInternal(children - 1)
      builder.setSize(size(node) - 1)
      builder.copy(node, 1, insertionPoint)
      builder.copy(node, insertionPoint + 1, childIndex)
      builder.insertChild(merged)
      builder.copy(node, childIndex + 2, node.length)
      builder.result()
    }
    private def replaceUpdatedChildren(node: N, insertionPoint: Int, left: Node[L, A], middle: A, right: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = insertionPoint + children
      builder.allocInternal(children)
      builder.setSize(size(node) - 1)
      builder.copy(node, 1, insertionPoint)
      builder.insertValue(middle)
      builder.copy(node, insertionPoint + 1, childIndex)
      builder.insertChild(left)
      builder.insertChild(right)
      builder.copy(node, childIndex + 2, node.length)
      builder.result()
    }
    override def rebalance(left: N, right: N)(implicit builder: NodeBuilder[Next[L], A]): Either[N, (N, A, N)] = {
      val leftCount = valueCount(left)
      val rightCount = valueCount(right)
      val highestLeftChild = left(1 + leftCount + leftCount).asInstanceOf[ChildNode]
      val lowestRightChild = right(1 + rightCount).asInstanceOf[ChildNode]
      childOps.rebalance(highestLeftChild, lowestRightChild)(builder.down) match {
        case Right((l, m, r)) =>
          builder.allocInternal(leftCount)
          builder.copy(left, 1, left.length - 1)
          builder.insertChild(l)
          builder.recalculateSize()
          val updatedLeft = builder.result()
          builder.allocInternal(rightCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.insertChild(r)
          builder.copy(right, 1 + rightCount + 1, right.length)
          builder.recalculateSize()
          val updatedRight = builder.result()
          Right((updatedLeft, m, updatedRight))
        case Left(merged) =>
          if (leftCount == halfOrder && rightCount == halfOrder) {
            builder.allocInternal(order)
            builder.setSize(size(left) + size(right))
            builder.copy(left, 1, 1 + leftCount)
            builder.copy(right, 1, 1 + rightCount)
            builder.copy(left, 1 + leftCount, left.length - 1)
            builder.insertChild(merged)
            builder.copy(right, 1 + rightCount + 1, right.length)
            Left(builder.result())
          } else if (valueCount(left) > valueCount(right)) {
            builder.allocInternal(leftCount - 1)
            builder.copy(left, 1, 1 + leftCount - 1)
            builder.copy(left, 1 + leftCount, left.length - 1)
            builder.recalculateSize()
            val updatedLeft = builder.result()
            val middle = left(leftCount).asInstanceOf[A]
            builder.allocInternal(rightCount)
            builder.copy(right, 1, 1 + rightCount)
            builder.insertChild(merged)
            builder.copy(right, 1 + rightCount + 1, right.length)
            builder.recalculateSize()
            val updatedRight = builder.result()
            Right((updatedLeft, middle, updatedRight))
          } else {
            builder.allocInternal(leftCount)
            builder.copy(left, 1, left.length - 1)
            builder.insertChild(merged)
            builder.recalculateSize()
            val updatedLeft = builder.result()
            val middle = right(1).asInstanceOf[A]
            builder.allocInternal(rightCount - 1)
            builder.copy(right, 2, 1 + rightCount)
            builder.copy(right, 1 + rightCount + 1, right.length)
            builder.recalculateSize()
            val updatedRight = builder.result()
            Right((updatedLeft, middle, updatedRight))
          }
      }
    }

    private def replaceMergedChildAndMergeWithLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = index + children
      builder.allocInternal(order)
      builder.setSize(size(leftSibling) + size(node))
      builder.copy(leftSibling, 1, 1 + valueCount(leftSibling))
      builder.insertValue(leftValue)
      builder.copy(node, 1, index)
      builder.copy(node, index + 1, 1 + children)
      builder.copy(leftSibling, 1 + valueCount(leftSibling), leftSibling.length)
      builder.copy(node, 1 + children, childIndex)
      builder.insertChild(merged)
      builder.copy(node, childIndex + 2, node.length)
      builder.result()
    }
    private def replaceMergedChildAndMergeWithRight(node: N, rightValue: A, rightSibling: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = index + children
      builder.allocInternal(order)
      builder.setSize(size(node) + size(rightSibling))
      builder.copy(node, 1, index)
      builder.copy(node, index + 1, 1 + children)
      builder.insertValue(rightValue)
      builder.copy(rightSibling, 1, 1 + valueCount(rightSibling))
      builder.copy(node, 1 + children, childIndex)
      builder.insertChild(merged)
      builder.copy(node, childIndex + 2, node.length)
      builder.copy(rightSibling, 1 + valueCount(rightSibling), rightSibling.length)
      builder.result()
    }
    private def replaceMergedChildAndTakeFromLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): (N, A, N) = {
      val leftCount = valueCount(leftSibling)
      builder.allocInternal(leftCount - 1)
      builder.copy(leftSibling, 1, 1 + leftCount - 1)
      builder.copy(leftSibling, 1 + leftCount, leftSibling.length - 1)
      builder.recalculateSize()
      val updatedLeft = builder.result()
      val updatedMiddle = leftSibling(leftCount).asInstanceOf[A]
      val children = valueCount(node)
      val childIndex = index + children
      builder.allocInternal(children)
      builder.insertValue(leftValue)
      builder.copy(node, 1, index)
      builder.copy(node, index + 1, 1 + children)
      builder.insertChild(rightChild(leftSibling, leftCount))
      builder.copy(node, 1 + children, childIndex)
      builder.insertChild(merged)
      builder.copy(node, childIndex + 2, node.length)
      builder.recalculateSize()
      val updatedRight = builder.result()
      (updatedLeft, updatedMiddle, updatedRight)
    }
    private def replaceMergedChildAndTakeFromRight(node: N, rightValue: A, rightSibling: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): (N, A, N) = {
      val children = valueCount(node)
      val childIndex = index + children
      builder.allocInternal(children)
      builder.copy(node, 1, index)
      builder.copy(node, index + 1, 1 + children)
      builder.insertValue(rightValue)
      builder.copy(node, 1 + children, childIndex)
      builder.insertChild(merged)
      builder.copy(node, childIndex + 2, node.length)
      builder.insertChild(leftChild(rightSibling, 1))
      builder.recalculateSize()
      val updatedLeft = builder.result()
      val updatedMiddle = rightSibling(1).asInstanceOf[A]
      val rightCount = valueCount(rightSibling)
      builder.allocInternal(rightCount - 1)
      builder.copy(rightSibling, 2, 1 + rightCount)
      builder.copy(rightSibling, 1 + rightCount + 1, rightSibling.length)
      builder.recalculateSize()
      val updatedRight = builder.result()
      (updatedLeft, updatedMiddle, updatedRight)
    }

    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      val children = valueCount(node)
      if (index >= 0) {
        val childIndex = index + children
        val left = leftChild(node, index)
        val right = rightChild(node, index)
        childOps.rebalance(left, right)(builder.down) match {
          case Left(merged) if children == halfOrder && valueCount(leftSibling) == halfOrder =>
            Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, index, merged)))
          case Left(merged) if children > halfOrder =>
            Some(Left(replaceMergedChildren(node, index, merged)))
          case Left(merged) if valueCount(leftSibling) > halfOrder =>
            Some(Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, index, merged)))
          case Right((l, m, r)) =>
            Some(Right((leftSibling, leftValue, replaceUpdatedChildren(node, index, l, m, r))))
        }
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val middleChild = child(node, childIndex)
        val childLeftSibling = if (childIndex > 1 + children) child(node, childIndex - 1) else null
        val childRightSibling = if (childIndex + 1 < node.length) child(node, childIndex + 1) else null
        val childLeftValue = if (insertionPoint > 1) node(insertionPoint - 1) else null
        val childRightValue = if (insertionPoint < 1 + children) node(insertionPoint) else null
        if (childLeftSibling == null || (childRightSibling != null && childOps.valueCount(childLeftSibling) < childOps.valueCount(childRightSibling))) {
          childOps.deleteAndMergeRight(middleChild, childRightValue.asInstanceOf[A], childRightSibling, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == halfOrder && valueCount(leftSibling) == halfOrder =>
              Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, insertionPoint, merged)))
            case Some(Left(merged)) if children > halfOrder =>
              Some(Right((leftSibling, leftValue, replaceMergedChildren(node, insertionPoint, merged))))
            case Some(Left(merged)) if valueCount(leftSibling) > halfOrder =>
              Some(Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, insertionPoint, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((leftSibling, leftValue, replaceUpdatedChildren(node, insertionPoint, l, m, r))))
          }
        } else {
          childOps.deleteAndMergeLeft(childLeftSibling, childLeftValue.asInstanceOf[A], middleChild, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == halfOrder && valueCount(leftSibling) == halfOrder =>
              Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, insertionPoint - 1, merged)))
            case Some(Left(merged)) if children > halfOrder =>
              Some(Right((leftSibling, leftValue, replaceMergedChildren(node, insertionPoint - 1, merged))))
            case Some(Left(merged)) if valueCount(leftSibling) > halfOrder =>
              Some(Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, insertionPoint - 1, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((leftSibling, leftValue, replaceUpdatedChildren(node, insertionPoint - 1, l, m, r))))
          }
        }
      }
    }
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      val children = valueCount(node)
      if (index >= 0) {
        val childIndex = index + children
        val left = leftChild(node, index)
        val right = rightChild(node, index)
        childOps.rebalance(left, right)(builder.down) match {
          case Left(merged) if children == halfOrder && valueCount(rightSibling) == halfOrder =>
            Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, index, merged)))
          case Left(merged) if children > halfOrder =>
            Some(Left(replaceMergedChildren(node, index, merged)))
          case Left(merged) if valueCount(rightSibling) > halfOrder =>
            Some(Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, index, merged)))
          case Right((l, m, r)) =>
            Some(Right((replaceUpdatedChildren(node, index, l, m, r), rightValue, rightSibling)))
        }
      } else {
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val middleChild = child(node, childIndex)
        val childLeftSibling = if (childIndex > 1 + children) child(node, childIndex - 1) else null
        val childRightSibling = if (childIndex + 1 < node.length) child(node, childIndex + 1) else null
        val childLeftValue = if (insertionPoint > 1) node(insertionPoint - 1) else null
        val childRightValue = if (insertionPoint < 1 + children) node(insertionPoint) else null
        if (childLeftSibling == null || (childRightSibling != null && childOps.valueCount(childLeftSibling) < childOps.valueCount(childRightSibling))) {
          childOps.deleteAndMergeRight(middleChild, childRightValue.asInstanceOf[A], childRightSibling, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == halfOrder && valueCount(rightSibling) == halfOrder =>
              Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, insertionPoint, merged)))
            case Some(Left(merged)) if children > halfOrder =>
              Some(Right((replaceMergedChildren(node, insertionPoint, merged), rightValue, rightSibling)))
            case Some(Left(merged)) if valueCount(rightSibling) > halfOrder =>
              Some(Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, insertionPoint, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((replaceUpdatedChildren(node, insertionPoint, l, m, r), rightValue, rightSibling)))
          }
        } else {
          childOps.deleteAndMergeLeft(childLeftSibling, childLeftValue.asInstanceOf[A], middleChild, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == halfOrder && valueCount(rightSibling) == halfOrder =>
              Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, insertionPoint - 1, merged)))
            case Some(Left(merged)) if children > halfOrder =>
              Some(Right((replaceMergedChildren(node, insertionPoint - 1, merged), rightValue, rightSibling)))
            case Some(Left(merged)) if valueCount(rightSibling) > halfOrder =>
              Some(Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, insertionPoint - 1, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((replaceUpdatedChildren(node, insertionPoint - 1, l, m, r), rightValue, rightSibling)))
          }
        }
      }
    }

    override def buildCollection(builder: Builder[A, _], node: N): Unit = {
      val children = valueCount(node)
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
    override def +(a: A) = {
      val builder = ops.newBuilder
      ops.insert(root, a)(builder) match {
        case Left(node)                   => new Root[ops.Level, A](node)
        case Right((left, middle, right)) => new Root[ops.NextLevel, A](builder.nonLeaf(left, middle, right))
      }
    }
    override def -(a: A): BTree[A] = {
      ops.deleteFromRoot(root, a)(ops.newBuilder) match {
        case None                => this
        case Some(Left(updated)) => new Root[L, A](updated)
        case Some(Right(child))  => new Root[ops.PreviousLevel, A](child)(implicitly[Ordering[A]], ops.myChildOps)
      }
    }
    override def isEmpty: Boolean = size == 0
    override def nonEmpty: Boolean = !isEmpty
    override def size: Int = ops.size(root)
    override def contains(a: A): Boolean = ops.contains(root, a)
    override def toVector: Vector[A] = ops.toVector(root)
    override def toString = toVector.mkString("BTree(", ",", ")")
  }
}

trait BTree[A] {
  def +(a: A): BTree[A]
  def -(a: A): BTree[A]
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
