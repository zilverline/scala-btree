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
    final def leaf(n: Int)(implicit ev: L =:= Leaf): this.type = {
      node = new Array[AnyRef](n).asInstanceOf[Node[L, A]]
      index = 0
      this
    }
    final def internal(n: Int)(implicit ev: L <:< Next[_]): this.type = {
      node = new Array[AnyRef](1 + n + n + 1).asInstanceOf[Node[L, A]]
      index = 1
      this
    }
    final def allocCopy(n: Node[L, A]): this.type = allocCopy(n, 0, n.length)
    final def allocCopy(n: Node[L, A], from: Int, to: Int): this.type = {
      node = Arrays.copyOfRange(n, from, to).asInstanceOf[Node[L, A]]
      this
    }

    final def copy(source: Node[L, A]): this.type = copy(source, 0, source.length)
    final def copy(source: Node[L, A], from: Int, to: Int): this.type = {
      //    assert(target >= 0, s"$target >= 0")
      //    assert(length >= 0, s"$length >= 0")
      //    assert(target + length <= dest.length, s"$target + $length <= ${dest.length}")
      var i = 0
      while (i < to - from) {
        node(index + i) = source(from + i)
        i += 1
      }
      index += to - from
      this
    }

    final def copyDeleted(source: Node[L, A], index: Int): this.type =
      copy(source, 0, index).copy(source, index + 1, source.length)

    final def insertValue(v: A): this.type = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
      this
    }
    final def insertChild[M](v: Node[M, A])(implicit ev: L =:= Next[M]): this.type = {
      //    assert(i >= 0, s"$i >= 0")
      //    assert(i < dest.length, s"$i < ${dest.length}")
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
      this
    }
    final def updateValue(index: Int, value: A): this.type = {
      node(index) = value.asInstanceOf[AnyRef]
      this
    }
    final def updateChild[M](index: Int, child: Node[M, A])(implicit ev: L =:= Next[M]): this.type = {
      node(index) = child.asInstanceOf[AnyRef]
      this
    }

    final def setSize(size: Int)(implicit ev: L <:< Next[_]): this.type = {
      node(0) = size.asInstanceOf[AnyRef]
      this
    }
    final def recalculateSize()(implicit ev: L <:< Next[_], ops: NodeOps[L, A]): this.type = {
      val children = ops.valueCount(node)
      var size = children
      var i = 1 + children
      while (i < node.length) {
        size += ops.childOps.size(ops.child(node, i))
        i += 1
      }
      node(0) = size.asInstanceOf[AnyRef]
      this
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

  private def valueComparator[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = ordering.asInstanceOf[Comparator[AnyRef]]

  /*
   * Leaves are represented by an Array[AnyRef]. Each element in the array is a value.
   * The length of the array is equal to the number of stored values.
   * There is no additional overhead (counts, child pointers).
   */
  implicit def LeafOperations[A: Ordering](implicit parameters: BTree.Parameters) = new LeafOperations()
  class LeafOperations[A]()(implicit val ordering: Ordering[A], val parameters: BTree.Parameters) extends NodeOps[Leaf, A] {
    override type PreviousLevel = Nothing

    def childOps: ChildOps = throw new RuntimeException("no child ops for leaf node")
    def child(node: N, index: Int): ChildNode = throw new RuntimeException("no child for leaf node")

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
        Left(builder.allocCopy(node).updateValue(index, a).result())
      } else {
        val insertionPoint = -index - 1
        if (valueCount(node) < maxValues) {
          Left(valueInserted(node, 0, node.length, insertionPoint, a))
        } else {
          Right(if (insertionPoint < minValues) {
            val left = valueInserted(node, 0, minValues - 1, insertionPoint, a)
            val middle = value(node, minValues - 1)
            val right = copied(node, minValues, maxValues)
            (left, middle, right)
          } else if (insertionPoint > minValues) {
            val left = copied(node, 0, minValues)
            val middle = value(node, minValues)
            val right = valueInserted(node, minValues + 1, maxValues, insertionPoint, a)
            (left, middle, right)
          } else {
            val left = copied(node, 0, minValues)
            val middle = a
            val right = copied(node, minValues, maxValues)
            (left, middle, right)
          })
        }
      }
    }

    override def deleteFromRoot(node: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, ChildNode]] = {
      val index = search(node, a)
      if (index < 0) None
      else Some(Left(valueDeleted(node, index)))
    }

    override def rebalance(left: N, right: N)(implicit builder: NodeBuilder[Leaf, A]): Either[N, (N, A, N)] =
      if (left.length == minValues && right.length == minValues)
        Left(builder.leaf(left.length + right.length).copy(left).copy(right).result())
      else
        Right(
          if (left.length > right.length)
            (valueDeleted(left, left.length - 1), value(left, left.length - 1), right)
          else
            (left, value(right, 0), valueDeleted(right, 0)))

    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index < 0)
        None
      else if (valueCount(node) > minValues) {
        Some(Right((leftSibling, leftValue, valueDeleted(node, index))))
      } else if (valueCount(leftSibling) > minValues) {
        val left = valueDeleted(leftSibling, leftSibling.length - 1)
        val middle = value(leftSibling, leftSibling.length - 1)
        val right = builder.leaf(node.length).insertValue(leftValue).copyDeleted(node, index).result()
        Some(Right((left, middle, right)))
      } else {
        Some(Left(builder.leaf(maxValues)
          .copy(leftSibling)
          .insertValue(leftValue)
          .copyDeleted(node, index).result()))
      }
    }
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: NodeBuilder[Leaf, A]): Option[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index < 0)
        None
      else if (valueCount(node) > minValues) {
        Some(Right((valueDeleted(node, index), rightValue, rightSibling)))
      } else if (valueCount(rightSibling) > minValues) {
        val left = builder.leaf(node.length).copyDeleted(node, index).insertValue(rightValue).result()
        val middle = value(rightSibling, 0)
        val right = valueDeleted(rightSibling, 0)
        Some(Right((left, middle, right)))
      } else {
        Some(Left(builder.leaf(maxValues)
          .copyDeleted(node, index)
          .insertValue(rightValue)
          .copy(rightSibling).result()))
      }
    }

    override def toVector(node: N): Vector[A] = node.asInstanceOf[Array[A]].toVector

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, a.asInstanceOf[AnyRef], valueComparator[A])

    private def copied(source: N, from: Int, to: Int)(implicit builder: NodeBuilder[Leaf, A]): N =
      builder.allocCopy(source, from, to).result()

    private def valueInserted(source: N, from: Int, to: Int, position: Int, value: A)(implicit builder: NodeBuilder[Leaf, A]): N =
      builder.leaf(to - from + 1)
        .copy(source, from, position)
        .insertValue(value)
        .copy(source, position, to)
        .result()

    private def valueDeleted(node: N, index: Int)(implicit builder: NodeBuilder[Leaf, A]): N =
      builder.leaf(node.length - 1).copyDeleted(node, index).result()
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

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, 1, valueCount(node) + 1, a.asInstanceOf[AnyRef], valueComparator[A])

    override def insert(node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Either[N, (N, A, N)] = {
      val index = search(node, a)
      if (index >= 0) {
        Left(builder.allocCopy(node).updateValue(index, a).result())
      } else {
        val children = valueCount(node)
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = node(childIndex).asInstanceOf[childOps.N]
        childOps.insert(child, a)(builder.down) match {
          case Left(updatedChild) =>
            builder.allocCopy(node)
            builder.updateChild(childIndex, updatedChild)
            val sizeChange = childOps.size(updatedChild) - childOps.size(child)
            builder.setSize(size(node) + sizeChange)
            Left(builder.result())
          case Right((left, middle, right)) =>
            if (children < maxValues) {
              builder.internal(children + 1)
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
                  builder.internal(minValues)
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
                  builder.internal(minValues)
                  builder.copy(node, 1 + minValues, 1 + children)
                  builder.copy(node, 1 + children + minValues, node.length)
                  builder.recalculateSize()
                  builder.result()
                }
                (l, m, r)
              } else if (insertionPoint > minValues + 1) {
                val l: N = {
                  builder.internal(minValues)
                  builder.copy(node, 1, 1 + minValues)
                  builder.copy(node, 1 + children, 1 + children + minValues + 1)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = node(minValues + 1).asInstanceOf[A]
                val r: N = {
                  builder.internal(minValues)
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
                  builder.internal(minValues)
                  builder.copy(node, 1, 1 + minValues)
                  builder.copy(node, 1 + maxValues, 1 + maxValues + minValues)
                  builder.insertChild(left)
                  builder.recalculateSize()
                  builder.result()
                }
                val m: A = middle
                val r: N = {
                  builder.internal(minValues)
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
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case None =>
          None
        case Some(Left(merged)) if children == 1 =>
          Some(Right(merged))
        case Some(Left(merged)) if children > 1 =>
          Some(Left(replaceMergedChildren(node, index, merged)))
        case Some(Right((left, middle, right))) =>
          Some(Left(replaceUpdatedChildren(node, index, left, middle, right)))
      }
    }
    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Option[Either[N, (N, A, N)]] = {
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case None =>
          None
        case Some(Left(merged)) if children == minValues && valueCount(leftSibling) == minValues =>
          Some(Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, index, merged)))
        case Some(Left(merged)) if children > minValues =>
          Some(Right((leftSibling, leftValue, replaceMergedChildren(node, index, merged))))
        case Some(Left(merged)) if valueCount(leftSibling) > minValues =>
          Some(Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, index, merged)))
        case Some(Right((left, middle, right))) =>
          Some(Right((leftSibling, leftValue, replaceUpdatedChildren(node, index, left, middle, right))))
      }
    }
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): Option[Either[N, (N, A, N)]] = {
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case None =>
          None
        case Some(Left(merged)) if children == minValues && valueCount(rightSibling) == minValues =>
          Some(Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, index, merged)))
        case Some(Left(merged)) if children > minValues =>
          Some(Right((replaceMergedChildren(node, index, merged), rightValue, rightSibling)))
        case Some(Left(merged)) if valueCount(rightSibling) > minValues =>
          Some(Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, index, merged)))
        case Some(Right((left, middle, right))) =>
          Some(Right((replaceUpdatedChildren(node, index, left, middle, right), rightValue, rightSibling)))
      }
    }
    override def rebalance(left: N, right: N)(implicit builder: NodeBuilder[Next[L], A]): Either[N, (N, A, N)] = {
      val leftCount = valueCount(left)
      val rightCount = valueCount(right)
      val highestLeftChild = left(1 + leftCount + leftCount).asInstanceOf[ChildNode]
      val lowestRightChild = right(1 + rightCount).asInstanceOf[ChildNode]
      childOps.rebalance(highestLeftChild, lowestRightChild)(builder.down) match {
        case Right((l, m, r)) =>
          builder.internal(leftCount)
          builder.copy(left, 1, left.length - 1)
          builder.insertChild(l)
          builder.recalculateSize()
          val updatedLeft = builder.result()
          builder.internal(rightCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.insertChild(r)
          builder.copy(right, 1 + rightCount + 1, right.length)
          builder.recalculateSize()
          val updatedRight = builder.result()
          Right((updatedLeft, m, updatedRight))
        case Left(merged) if leftCount == minValues && rightCount == minValues =>
          builder.internal(maxValues)
          builder.setSize(size(left) + size(right))
          builder.copy(left, 1, 1 + leftCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.copy(left, 1 + leftCount, left.length - 1)
          builder.insertChild(merged)
          builder.copy(right, 1 + rightCount + 1, right.length)
          Left(builder.result())
        case Left(merged) if leftCount > rightCount =>
          builder.internal(leftCount - 1)
          builder.copy(left, 1, 1 + leftCount - 1)
          builder.copy(left, 1 + leftCount, left.length - 1)
          builder.recalculateSize()
          val updatedLeft = builder.result()
          val middle = left(leftCount).asInstanceOf[A]
          builder.internal(rightCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.insertChild(merged)
          builder.copy(right, 1 + rightCount + 1, right.length)
          builder.recalculateSize()
          val updatedRight = builder.result()
          Right((updatedLeft, middle, updatedRight))
        case Left(merged) =>
          builder.internal(leftCount)
          builder.copy(left, 1, left.length - 1)
          builder.insertChild(merged)
          builder.recalculateSize()
          val updatedLeft = builder.result()
          val middle = right(1).asInstanceOf[A]
          builder.internal(rightCount - 1)
          builder.copy(right, 2, 1 + rightCount)
          builder.copy(right, 1 + rightCount + 1, right.length)
          builder.recalculateSize()
          val updatedRight = builder.result()
          Right((updatedLeft, middle, updatedRight))
      }
    }

    private def deleteValue(node: N, a: A)(implicit builder: NodeBuilder[Next[L], A]): (Int, Option[Either[ChildNode, (ChildNode, A, ChildNode)]]) = {
      val index = search(node, a)
      if (index >= 0) {
        (index, Some(childOps.rebalance(leftChild(node, index), rightChild(node, index))(builder.down)))
      } else {
        val insertionPoint = -index - 1
        val children = valueCount(node)
        val childIndex = insertionPoint + children
        val middle = child(node, childIndex)
        val left = if (childIndex > 1 + children) child(node, childIndex - 1) else null
        val right = if (childIndex + 1 < node.length) child(node, childIndex + 1) else null
        if (left == null || (right != null && childOps.valueCount(left) < childOps.valueCount(right))) {
          (insertionPoint, childOps.deleteAndMergeRight(middle, node(insertionPoint).asInstanceOf[A], right, a)(builder.down))
        } else {
          (insertionPoint - 1, childOps.deleteAndMergeLeft(left, node(insertionPoint - 1).asInstanceOf[A], middle, a)(builder.down))
        }
      }
    }

    private def replaceMergedChildren(node: N, insertionPoint: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = insertionPoint + children
      builder.internal(children - 1)
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
      builder.internal(children)
      builder.setSize(size(node) - 1)
      builder.copy(node, 1, insertionPoint)
      builder.insertValue(middle)
      builder.copy(node, insertionPoint + 1, childIndex)
      builder.insertChild(left)
      builder.insertChild(right)
      builder.copy(node, childIndex + 2, node.length)
      builder.result()
    }
    private def replaceMergedChildAndMergeWithLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: NodeBuilder[Next[L], A]): N = {
      val children = valueCount(node)
      val childIndex = index + children
      builder.internal(maxValues)
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
      builder.internal(maxValues)
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
      builder.internal(leftCount - 1)
      builder.copy(leftSibling, 1, 1 + leftCount - 1)
      builder.copy(leftSibling, 1 + leftCount, leftSibling.length - 1)
      builder.recalculateSize()
      val updatedLeft = builder.result()
      val updatedMiddle = leftSibling(leftCount).asInstanceOf[A]
      val children = valueCount(node)
      val childIndex = index + children
      builder.internal(children)
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
      builder.internal(children)
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
      builder.internal(rightCount - 1)
      builder.copy(rightSibling, 2, 1 + rightCount)
      builder.copy(rightSibling, 1 + rightCount + 1, rightSibling.length)
      builder.recalculateSize()
      val updatedRight = builder.result()
      (updatedLeft, updatedMiddle, updatedRight)
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
          rootBuilder.internal(1)
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
