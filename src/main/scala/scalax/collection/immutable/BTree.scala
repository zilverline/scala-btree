package scalax.collection.immutable

import java.util.Arrays
import java.util.Comparator
import scala.collection.mutable.Builder

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

  case class Parameters(minLeafValues: Int = 16, minInternalValues: Int = 16)

  val DefaultParameters = Parameters()

  def apply[A: Ordering](elements: A*)(implicit parameters: Parameters = DefaultParameters): BTree[A] = elements.foldLeft(BTree.empty[A])(_ + _)
  def empty[A: Ordering](implicit parameters: Parameters = DefaultParameters): BTree[A] = new Root[Leaf, A](Array.empty[AnyRef].asInstanceOf[Node[Leaf, A]])(implicitly, LeafOperations)
}

private[immutable] object implementation {
  /* Track the node level in the type system to keep the tree balanced. */
  sealed trait Level
  final class Leaf extends Level
  final class Next[L] extends Level

  /* Since nodes are represented by raw Array[AnyRef], tag nodes with the level and element type. */
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  /* Nodes are raw arrays (for memory efficiency) but tagged for compile time checking. */
  type Node[L, A] = Array[AnyRef] @@ (L, A)

  /* Helper to build new nodes. `NodeBuilder`s are mutable so be careful when using! */
  class NodeBuilder[L, A] {
    private var node: Node[L, A] = _
    private var index: Int = _

    private def invariant() = {
      assert(node.forall(_ ne null), s"contains null: ${node.mkString("(", ",", ")")}")
    }

    final def result(): Node[L, A] = {
      //      invariant()
      val r = node
      node = null
      index = -1
      r
    }
    final def allocLeaf(n: Int)(implicit ev: L =:= Leaf): Unit = {
      node = new Array[AnyRef](n).asInstanceOf[Node[L, A]]
      index = 0
    }
    final def allocInternal(n: Int)(implicit ev: L <:< Next[_]): Unit = {
      node = new Array[AnyRef](1 + n + n + 1).asInstanceOf[Node[L, A]]
      index = 1
    }
    final def allocCopy(n: Node[L, A]): Unit = {
      node = Arrays.copyOf(n, n.length).asInstanceOf[Node[L, A]]
    }

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

    final def setSize(size: Int)(implicit ev: L <:< Next[_]): Unit = node(0) = size.asInstanceOf[AnyRef]
    final def recalculateSize()(implicit ev: L <:< Next[_], ops: NodeOps[L, A]): Unit = {
      val children = ops.valueCount(node)
      var size = children
      var i = 1 + children
      while (i < node.length) {
        size += ops.childOps.size(ops.child(node, i))
        i += 1
      }
      node(0) = size.asInstanceOf[AnyRef]
    }

    /* Cast this instance to a builder for nodes of the parent level. */
    final def up: NodeBuilder[Next[L], A] = this.asInstanceOf[NodeBuilder[Next[L], A]]
    /* Cast this instance to a builder for nodes of the child level. */
    final def down[M](implicit ev: L =:= Next[M]): NodeBuilder[M, A] = this.asInstanceOf[NodeBuilder[M, A]]
  }

  /*
   * Since nodes are raw Java arrays we cannot extend them with the operations we need.
   * Instead, we keep a stack of NodeOps with one instance for each level in the tree. At the bottom of the
   * stack we keep an instance of `LeafOperations`, while all others are instances of `InternalOperations`.
   * This also lets us know the layout of the array we're working with.
   */
  sealed trait NodeOps[L, A] {
    type N = Node[L, A]

    type Level = L
    type NextLevel = Next[L]
    type PreviousLevel

    type ChildNode = Node[PreviousLevel, A]
    type ChildOps = NodeOps[PreviousLevel, A]

    def parameters: BTree.Parameters
    def ordering: Ordering[A]

    def newBuilder = new NodeBuilder[L, A]

    def valueCount(node: N): Int

    def childOps: ChildOps

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

  private def castingOrdering[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = ordering.asInstanceOf[Comparator[AnyRef]]

  /*
   * Leaves are represented by an Array[AnyRef]. Each element in the array is a value.
   * The length of the array is equal to the number of stored values.
   * There is no additional overhead (counts, child pointers).
   */
  implicit def LeafOperations[A: Ordering](implicit parameters: BTree.Parameters) = new LeafOperations()
  class LeafOperations[A]()(implicit val ordering: Ordering[A], val parameters: BTree.Parameters) extends NodeOps[Leaf, A] {
    override type PreviousLevel = Nothing

    def childOps: ChildOps = throw new RuntimeException("no child ops for leaf node")
    def child(node: N, index: Int): ChildNode = throw new RuntimeException("no child ops for leaf node")

    val minValues = parameters.minLeafValues
    val maxValues = minValues * 2

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
        if (size(node) < maxValues) {
          builder.allocLeaf(node.length + 1)
          builder.copy(node, 0, insertionPoint)
          builder.insertValue(a)
          builder.copy(node, insertionPoint, node.length)
          Left(builder.result())
        } else {
          Right(if (insertionPoint < minValues) {
            val left: N = copyAndInsertValue(node, 0, minValues - 1, insertionPoint, a)
            val middle: A = node(minValues - 1).asInstanceOf[A]
            val right: N = copyOfRange(node, minValues, maxValues)
            (left, middle, right)
          } else if (insertionPoint > minValues) {
            val left: N = copyOfRange(node, 0, minValues)
            val middle: A = node(minValues).asInstanceOf[A]
            val right: N = copyAndInsertValue(node, minValues + 1, maxValues, insertionPoint, a)
            (left, middle, right)
          } else {
            val left: N = copyOfRange(node, 0, minValues)
            val middle: A = a
            val right: N = copyOfRange(node, minValues, maxValues)
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
      if (left.length == minValues && right.length == minValues) {
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
      else if (values > minValues) {
        builder.allocLeaf(values - 1)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Right((leftSibling, leftValue, builder.result())))
      } else if (valueCount(leftSibling) > minValues) {
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
        builder.allocLeaf(maxValues)
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
      else if (values > minValues) {
        builder.allocLeaf(values - 1)
        builder.copy(node, 0, index)
        builder.copy(node, index + 1, node.length)
        Some(Right((builder.result(), rightValue, rightSibling)))
      } else if (valueCount(rightSibling) > minValues) {
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
        builder.allocLeaf(maxValues)
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

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, a.asInstanceOf[AnyRef], castingOrdering[A])
  }

  /*
   * Internal nodes are represented by an Array[AnyRef]. An internal node with C values has the following layout.
   *
   * node(0)                      - a java.lang.Integer containing the total size of this subtree.
   * node(1) .. node(C)           - the values stored in this node.
   * node(1 + C) .. node(1 + 2*C) - the children of this internal node.
   */
  implicit def InternalOps[L, A](implicit childOps: NodeOps[L, A]): NodeOps[Next[L], A] = new InternalOperations()
  class InternalOperations[L, A]()(implicit val childOps: NodeOps[L, A]) extends NodeOps[Next[L], A] {
    implicit val ordering = childOps.ordering
    val parameters = childOps.parameters
    val minValues = parameters.minInternalValues
    val maxValues = minValues * 2

    override type ChildNode = childOps.N
    override type PreviousLevel = L

    override def valueCount(node: N): Int = (node.length - 2) / 2
    override def size(node: N): Int = {
      //assert(node(0) ne null, "size not set")
      node(0).asInstanceOf[Int]
    }
    override def contains(node: N, a: A): Boolean = {
      val index = search(node, a)
      index >= 0 || {
        val children = valueCount(node)
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node(childIndex).asInstanceOf[childOps.N]
        childOps.contains(child, a)
      }
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
            if (children < maxValues) {
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
              Right(if (insertionPoint < minValues + 1) {
                val l: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1, insertionPoint)
                  builder.insertValue(middle)
                  builder.copy(node, insertionPoint, 1 + minValues - 1)
                  builder.copy(node, 1 + children, childIndex)
                  builder.insertChild(left)
                  builder.insertChild(right)
                  builder.copy(node, childIndex + 1, 1 + maxValues + minValues)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = node(minValues).asInstanceOf[A]
                val r: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1 + minValues, 1 + children)
                  builder.copy(node, 1 + children + minValues, node.length)
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              } else if (insertionPoint > minValues + 1) {
                val l: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1, 1 + minValues)
                  builder.copy(node, 1 + children, 1 + children + minValues + 1)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = node(minValues + 1).asInstanceOf[A]
                val r: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1 + minValues + 1, insertionPoint)
                  builder.insertValue(middle)
                  builder.copy(node, insertionPoint, 1 + children)
                  builder.copy(node, 1 + children + minValues + 1, childIndex)
                  builder.insertChild(left)
                  builder.insertChild(right)
                  builder.copy(node, childIndex + 1, node.length)
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              } else {
                val l: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1, 1 + minValues)
                  builder.copy(node, 1 + maxValues, 1 + maxValues + minValues)
                  builder.insertChild(left)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = middle
                val r: N = {
                  builder.allocInternal(minValues)
                  builder.copy(node, 1 + minValues, 1 + maxValues)
                  builder.insertChild(right)
                  builder.copy(node, 1 + maxValues + minValues + 1, node.length)
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
        case Left(merged) if leftCount == minValues && rightCount == minValues =>
          builder.allocInternal(maxValues)
          builder.setSize(size(left) + size(right))
          builder.copy(left, 1, 1 + leftCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.copy(left, 1 + leftCount, left.length - 1)
          builder.insertChild(merged)
          builder.copy(right, 1 + rightCount + 1, right.length)
          Left(builder.result())
        case Left(merged) if leftCount > rightCount =>
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
        case Left(merged) =>
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

    private def replaceMergedChildAndMergeWithLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = index + children
      builder.allocInternal(maxValues)
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
      builder.allocInternal(maxValues)
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
          case Left(merged) if children == minValues && valueCount(leftSibling) == minValues =>
            Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, index, merged)))
          case Left(merged) if children > minValues =>
            Some(Right((leftSibling, leftValue, replaceMergedChildren(node, index, merged))))
          case Left(merged) if valueCount(leftSibling) > minValues =>
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
            case Some(Left(merged)) if children == minValues && valueCount(leftSibling) == minValues =>
              Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, insertionPoint, merged)))
            case Some(Left(merged)) if children > minValues =>
              Some(Right((leftSibling, leftValue, replaceMergedChildren(node, insertionPoint, merged))))
            case Some(Left(merged)) if valueCount(leftSibling) > minValues =>
              Some(Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, insertionPoint, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((leftSibling, leftValue, replaceUpdatedChildren(node, insertionPoint, l, m, r))))
          }
        } else {
          childOps.deleteAndMergeLeft(childLeftSibling, childLeftValue.asInstanceOf[A], middleChild, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == minValues && valueCount(leftSibling) == minValues =>
              Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, insertionPoint - 1, merged)))
            case Some(Left(merged)) if children > minValues =>
              Some(Right((leftSibling, leftValue, replaceMergedChildren(node, insertionPoint - 1, merged))))
            case Some(Left(merged)) if valueCount(leftSibling) > minValues =>
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
          case Left(merged) if children == minValues && valueCount(rightSibling) == minValues =>
            Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, index, merged)))
          case Left(merged) if children > minValues =>
            Some(Right((replaceMergedChildren(node, index, merged), rightValue, rightSibling)))
          case Left(merged) if valueCount(rightSibling) > minValues =>
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
            case Some(Left(merged)) if children == minValues && valueCount(rightSibling) == minValues =>
              Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, insertionPoint, merged)))
            case Some(Left(merged)) if children > minValues =>
              Some(Right((replaceMergedChildren(node, insertionPoint, merged), rightValue, rightSibling)))
            case Some(Left(merged)) if valueCount(rightSibling) > minValues =>
              Some(Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, insertionPoint, merged)))
            case Some(Right((l, m, r))) =>
              Some(Right((replaceUpdatedChildren(node, insertionPoint, l, m, r), rightValue, rightSibling)))
          }
        } else {
          childOps.deleteAndMergeLeft(childLeftSibling, childLeftValue.asInstanceOf[A], middleChild, a)(builder.down) match {
            case None =>
              None
            case Some(Left(merged)) if children == minValues && valueCount(rightSibling) == minValues =>
              Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, insertionPoint - 1, merged)))
            case Some(Left(merged)) if children > minValues =>
              Some(Right((replaceMergedChildren(node, insertionPoint - 1, merged), rightValue, rightSibling)))
            case Some(Left(merged)) if valueCount(rightSibling) > minValues =>
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
        case Left(node) => new Root(node)
        case Right((left, middle, right)) =>
          val rootBuilder = builder.up
          rootBuilder.allocInternal(1)
          rootBuilder.setSize(ops.size(left) + ops.size(right) + 1)
          rootBuilder.insertValue(middle)
          rootBuilder.insertChild(left)
          rootBuilder.insertChild(right)
          new Root(rootBuilder.result())
      }
    }
    override def -(a: A): BTree[A] = {
      ops.deleteFromRoot(root, a)(ops.newBuilder) match {
        case None                => this
        case Some(Left(updated)) => new Root(updated)
        case Some(Right(child))  => new Root(child)(ops.ordering, ops.childOps)
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
