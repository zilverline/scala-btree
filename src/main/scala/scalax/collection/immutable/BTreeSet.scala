package scalax.collection.immutable

import java.util.Arrays
import java.util.Comparator
import scala.collection.SortedSetLike
import scala.collection.generic.ImmutableSortedSetFactory
import scala.collection.immutable.SortedSet
import scala.collection.mutable.{ Builder, SetBuilder }

trait BTreeSet[A] extends SortedSet[A] with SortedSetLike[A, BTreeSet[A]] with Serializable {
  override def empty: BTreeSet[A] = BTreeSet.empty[A]

  // Should be an override in Scala 2.11
  def iteratorFrom(start: A): Iterator[A]
}
object BTreeSet extends ImmutableSortedSetFactory[BTreeSet] {
  import implementation._

  case class Parameters(minLeafValues: Int = 16, minInternalValues: Int = 16)

  val DefaultParameters = Parameters()

  def empty[A: Ordering]: BTreeSet[A] = new Root[Leaf, A](Array.empty[AnyRef].asInstanceOf[Node[Leaf, A]])(implicitly, LeafOperations(implicitly, DefaultParameters))
  def withParameters[A: Ordering](parameters: Parameters): BTreeSet[A] = new Root[Leaf, A](Array.empty[AnyRef].asInstanceOf[Node[Leaf, A]])(implicitly, LeafOperations(implicitly, parameters))
}

private[immutable] object implementation {
  /* Track the node level in the type system to keep the tree balanced. */
  sealed trait Level
  final class Leaf extends Level
  final class Next[L <: Level] extends Level

  /* Since nodes are represented by raw Array[AnyRef], tag nodes with the level and element type. */
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  /* Marker type to indicate a value can be null. For documentation only. */
  type Nullable[A >: Null] = A

  /* Nodes are raw arrays (for memory efficiency) but tagged for compile time checking. */
  type Node[L <: Level, A] = Array[AnyRef] @@ (L, A)

  /* Helper to build new nodes. `NodeBuilder`s are mutable so be careful when using! */
  class NodeBuilder[L <: Level, A] {
    private var node: Node[L, A] = _
    private var index: Int = _

    final def result(): Node[L, A] = {
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
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
      this
    }
    final def insertChild[M <: Level](v: Node[M, A])(implicit ev: L =:= Next[M]): this.type = {
      node(index) = v.asInstanceOf[AnyRef]
      index += 1
      this
    }
    final def updateValue(index: Int, value: A): this.type = {
      node(index) = value.asInstanceOf[AnyRef]
      this
    }
    final def updateChild[M <: Level](index: Int, child: Node[M, A])(implicit ev: L =:= Next[M]): this.type = {
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
        size += ops.childOps.size(ops.childAt(node, i))
        i += 1
      }
      node(0) = size.asInstanceOf[AnyRef]
      this
    }

    /* Cast this instance to a builder for nodes of the parent level. */
    final def up: NodeBuilder[Next[L], A] = this.asInstanceOf[NodeBuilder[Next[L], A]]
    /* Cast this instance to a builder for nodes of the child level. */
    final def down[M <: Level](implicit ev: L =:= Next[M]): NodeBuilder[M, A] = this.asInstanceOf[NodeBuilder[M, A]]
  }

  sealed trait NodeWithOps[A] {
    type L <: Level
    def ops: NodeOps[L, A]
    def node: Node[L, A]
  }
  def NodeWithOps[L0 <: Level, A](node0: Node[L0, A])(implicit ops0: NodeOps[L0, A]): NodeWithOps[A] = new NodeWithOps[A] {
    type L = L0
    def ops = ops0
    def node = node0
  }

  /*
   * Since nodes are raw Java arrays we cannot extend them with the operations we need.
   * Instead, we keep a stack of NodeOps with one instance for each level in the tree. At the bottom of the
   * stack we keep an instance of `LeafOperations`, while all others are instances of `InternalOperations`.
   * This also lets us know the layout of the array we're working with.
   */
  sealed trait NodeOps[L <: Level, A] {
    final type N = Node[L, A]

    type NextLevel = Next[L]
    type PreviousLevel <: Level

    type ChildNode = Node[PreviousLevel, A]
    type ChildOps = NodeOps[PreviousLevel, A]

    type Builder = NodeBuilder[L, A]

    def level: Int
    def parameters: BTreeSet.Parameters
    def ordering: Ordering[A]

    def newBuilder: Builder = new NodeBuilder[L, A]

    def valueCount(node: N): Int

    def childOps: ChildOps

    def valueAt(node: N, index: Int): A

    def childAt(node: N, index: Int): ChildNode
    def leftChild(node: N, index: Int): ChildNode
    def rightChild(node: N, index: Int): ChildNode

    def size(node: N): Int
    def isEmpty(node: N): Boolean

    def contains(node: N, a: A): Boolean
    def insert(node: N, a: A, overwrite: Boolean)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]]

    def pathToHead(node: N, parent: Nullable[PathNode[A]] = null): Nullable[PathNode[A]]
    def pathToKey(node: N, key: A, parent: Nullable[PathNode[A]] = null): Nullable[PathNode[A]]

    def deleteFromRoot(node: N, a: A)(implicit builder: Builder): Nullable[Either[N, ChildNode]]
    def rebalance(left: N, right: N)(implicit builder: Builder): Either[N, (N, A, N)]
    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]]
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]]

    def splitAtIndex(node: N, index: Int)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A])
    def splitAtKey(node: N, key: A)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A])

    def prepend(smallerTree: NodeWithOps[A], value: A, node: N)(implicit builder: Builder): Either[N, (N, A, N)]
    def append(node: N, value: A, smallerTree: NodeWithOps[A])(implicit builder: Builder): Either[N, (N, A, N)]

    def head(node: N): A
    def last(node: N): A
  }

  private def valueComparator[A](implicit ordering: Ordering[A]): Comparator[AnyRef] = ordering.asInstanceOf[Comparator[AnyRef]]

  /*
   * Leaves are represented by an Array[AnyRef]. Each element in the array is a value.
   * The length of the array is equal to the number of stored values.
   * There is no additional overhead (counts, child pointers).
   */
  implicit def LeafOperations[A: Ordering](implicit parameters: BTreeSet.Parameters) = new LeafOperations()
  final class LeafOperations[A]()(implicit val ordering: Ordering[A], val parameters: BTreeSet.Parameters) extends NodeOps[Leaf, A] {
    override type PreviousLevel = Nothing

    override def level = 0

    def childOps: ChildOps = throw new RuntimeException("no child ops for leaf node")
    def childAt(node: N, index: Int): ChildNode = throw new RuntimeException("no child for leaf node")
    def leftChild(node: N, index: Int): ChildNode = throw new RuntimeException("no child for leaf node")
    def rightChild(node: N, index: Int): ChildNode = throw new RuntimeException("no child for leaf node")

    val minValues = parameters.minLeafValues
    val maxValues = minValues * 2

    override def valueAt(node: N, index: Int): A = node(index).asInstanceOf[A]

    override def valueCount(node: N): Int = node.length
    override def size(node: N): Int = valueCount(node)
    override def isEmpty(node: N): Boolean = size(node) == 0
    override def contains(node: N, a: A): Boolean = search(node, a) >= 0

    override def insert(node: N, a: A, overwrite: Boolean)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index >= 0) {
        if (overwrite) Left(builder.allocCopy(node).updateValue(index, a).result()) else null
      } else {
        val insertionPoint = -index - 1
        if (valueCount(node) < maxValues) {
          Left(valueInserted(node, 0, node.length, insertionPoint, a))
        } else {
          Right(if (insertionPoint < minValues) {
            val l: N = valueInserted(node, 0, minValues - 1, insertionPoint, a)
            val m: A = valueAt(node, minValues - 1)
            val r: N = copied(node, minValues, maxValues)
            (l, m, r)
          } else if (insertionPoint > minValues) {
            val l: N = copied(node, 0, minValues)
            val m: A = valueAt(node, minValues)
            val r: N = valueInserted(node, minValues + 1, maxValues, insertionPoint, a)
            (l, m, r)
          } else {
            val l: N = copied(node, 0, minValues)
            val m: A = a
            val r: N = copied(node, minValues, maxValues)
            (l, m, r)
          })
        }
      }
    }

    override def deleteFromRoot(node: N, a: A)(implicit builder: Builder): Nullable[Either[N, ChildNode]] = {
      val index = search(node, a)
      if (index < 0) null
      else Left(valueDeleted(node, index))
    }

    override def rebalance(left: N, right: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      if (left.length == minValues && right.length == minValues)
        Left(builder.leaf(left.length + right.length).copy(left).copy(right).result())
      else
        Right(
          if (left.length > right.length)
            (valueDeleted(left, left.length - 1), valueAt(left, left.length - 1), right)
          else
            (left, valueAt(right, 0), valueDeleted(right, 0)))
    }

    override def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index < 0)
        null
      else if (valueCount(node) > minValues) {
        Right((leftSibling, leftValue, valueDeleted(node, index)))
      } else if (valueCount(leftSibling) > minValues) {
        val left = valueDeleted(leftSibling, leftSibling.length - 1)
        val middle = valueAt(leftSibling, leftSibling.length - 1)
        val right = builder.leaf(node.length).insertValue(leftValue).copyDeleted(node, index).result()
        Right((left, middle, right))
      } else {
        Left(builder.leaf(maxValues)
          .copy(leftSibling)
          .insertValue(leftValue)
          .copyDeleted(node, index).result())
      }
    }

    override def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index < 0)
        null
      else if (valueCount(node) > minValues) {
        Right((valueDeleted(node, index), rightValue, rightSibling))
      } else if (valueCount(rightSibling) > minValues) {
        val left = builder.leaf(node.length).copyDeleted(node, index).insertValue(rightValue).result()
        val middle = valueAt(rightSibling, 0)
        val right = valueDeleted(rightSibling, 0)
        Right((left, middle, right))
      } else {
        Left(builder.leaf(maxValues)
          .copyDeleted(node, index)
          .insertValue(rightValue)
          .copy(rightSibling).result())
      }
    }

    override def pathToHead(node: N, parent: Nullable[PathNode[A]]): Nullable[PathNode[A]] =
      if (isEmpty(node)) null else new PathLeafNode(node, 0, parent)

    override def pathToKey(node: N, key: A, parent: Nullable[PathNode[A]]): Nullable[PathNode[A]] = {
      val index = search(node, key)
      val startPoint = if (index >= 0) index else -(index + 1)
      if (startPoint >= valueCount(node)) null
      else new PathLeafNode(node, startPoint, parent)
    }

    override def splitAtIndex(node: N, index: Int)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A]) = {
      val left = builder.allocCopy(node, 0, index).result()
      val right = builder.allocCopy(node, index, node.length).result()
      (NodeWithOps(left), NodeWithOps(right))
    }

    override def splitAtKey(node: N, key: A)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A]) = {
      val index = search(node, key)
      val splitPoint = if (index >= 0) index else (-index - 1)
      splitAtIndex(node, splitPoint)
    }

    override def prepend(prefix: NodeWithOps[A], value: A, right: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      assert(prefix.ops.level == 0, "prefix must be leaf")
      val left = prefix.node.asInstanceOf[N]
      concatenate(left, value, right)
    }

    override def append(left: N, value: A, suffix: NodeWithOps[A])(implicit builder: Builder): Either[N, (N, A, N)] = {
      assert(suffix.ops.level == 0, "suffix must be leaf")
      val right = suffix.node.asInstanceOf[N]
      concatenate(left, value, right)
    }

    override def head(node: N): A = {
      if (isEmpty(node)) throw new NoSuchElementException("empty set")
      valueAt(node, 0)
    }

    override def last(node: N): A = {
      if (isEmpty(node)) throw new NoSuchElementException("empty set")
      valueAt(node, node.length - 1)
    }

    private[this] def concatenate(left: N, value: A, right: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      val leftCount = valueCount(left)
      val rightCount = valueCount(right)
      val totalCount = leftCount + 1 + rightCount
      if (totalCount <= maxValues) {
        Left(builder.leaf(totalCount).copy(left).insertValue(value).copy(right).result())
      } else {
        val newLeftCount = totalCount / 2
        val newRightCount = totalCount - 1 - newLeftCount
        Right(if (newLeftCount < leftCount) {
          val l: N = builder.leaf(newLeftCount).copy(left, 0, newLeftCount).result()
          val m: A = valueAt(left, newLeftCount)
          val r: N = builder.leaf(newRightCount).copy(left, newLeftCount + 1, leftCount).insertValue(value).copy(right).result()
          (l, m, r)
        } else if (newLeftCount > leftCount) {
          val l: N = builder.leaf(newLeftCount).copy(left).insertValue(value).copy(right, 0, newLeftCount - leftCount - 1).result()
          val m: A = valueAt(right, newLeftCount - leftCount - 1)
          val r: N = builder.leaf(newRightCount).copy(right, newLeftCount - leftCount, rightCount).result()
          (l, m, r)
        } else {
          (left, value, right)
        })
      }
    }

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, a.asInstanceOf[AnyRef], valueComparator[A])

    private def copied(source: N, from: Int, to: Int)(implicit builder: Builder): N =
      builder.allocCopy(source, from, to).result()

    private def valueInserted(source: N, from: Int, to: Int, position: Int, value: A)(implicit builder: Builder): N =
      builder.leaf(to - from + 1)
        .copy(source, from, position)
        .insertValue(value)
        .copy(source, position, to)
        .result()

    private def valueDeleted(node: N, index: Int)(implicit builder: Builder): N =
      builder.leaf(node.length - 1).copyDeleted(node, index).result()
  }

  /*
   * Internal nodes are represented by an Array[AnyRef]. An internal node with C values has the following layout.
   *
   * node(0)                      - a java.lang.Integer containing the total size of this subtree.
   * node(1) .. node(C)           - the values stored in this node.
   * node(1 + C) .. node(1 + 2*C) - the children of this internal node.
   */
  implicit def InternalOps[L <: Level, A](implicit childOps: NodeOps[L, A]): NodeOps[Next[L], A] = new InternalOperations()
  final case class InternalOperations[L <: Level, A]()(implicit val childOps: NodeOps[L, A]) extends NodeOps[Next[L], A] {
    implicit val ordering = childOps.ordering
    val parameters = childOps.parameters
    val minValues = parameters.minInternalValues
    val maxValues = minValues * 2

    override type ChildNode = childOps.N
    override type PreviousLevel = L
    override val level = childOps.level + 1

    override def valueCount(node: N): Int = (node.length >> 1) - 1
    override def size(node: N): Int = node(0).asInstanceOf[Int]
    override def isEmpty(node: N): Boolean = false

    override def contains(node: N, a: A): Boolean = {
      val index = search(node, a)
      index >= 0 || {
        val children = valueCount(node)
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = childAt(node, childIndex)
        childOps.contains(child, a)
      }
    }

    override def insert(node: N, a: A, overwrite: Boolean)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val index = search(node, a)
      if (index >= 0) {
        if (overwrite) Left(builder.allocCopy(node).updateValue(index, a).result()) else null
      } else {
        val children = valueCount(node)
        val insertionPoint = -index - 1
        val childIndex = insertionPoint + children
        val child = childAt(node, childIndex)
        childUpdatedOrSplit(node, insertionPoint, childOps.insert(child, a, overwrite)(builder.down))
      }
    }

    private def childUpdatedOrSplit(node: N, insertionPoint: Int, child: Nullable[Either[ChildNode, (ChildNode, A, ChildNode)]])(implicit builder: Builder): Either[N, (N, A, N)] = {
      val children = valueCount(node)
      val childIndex = insertionPoint + children
      child match {
        case null =>
          null
        case Left(updatedChild) =>
          val originalChild = childAt(node, childIndex)
          val sizeChange = childOps.size(updatedChild) - childOps.size(originalChild)
          builder.allocCopy(node)
          builder.setSize(size(node) + sizeChange)
          builder.updateChild(childIndex, updatedChild)
          Left(builder.result())
        case Right((left, middle, right)) if children < maxValues =>
          Left(valueInserted(node, 0, children, insertionPoint, left, middle, right))
        case Right((left, middle, right)) =>
          Right(split(node, insertionPoint, left, middle, right))
      }
    }

    private def split(node: N, insertionPoint: Int, left: ChildNode, middle: A, right: ChildNode)(implicit builder: Builder): (N, A, N) = {
      val children = valueCount(node)
      if (insertionPoint < minValues + 1) {
        val l: N = valueInserted(node, 0, minValues - 1, insertionPoint, left, middle, right)
        val m: A = valueAt(node, minValues)
        val r: N = copied(node, minValues, children)
        (l, m, r)
      } else if (insertionPoint > minValues + 1) {
        val l: N = copied(node, 0, minValues)
        val m: A = valueAt(node, minValues + 1)
        val r: N = valueInserted(node, minValues + 1, children, insertionPoint, left, middle, right)
        (l, m, r)
      } else {
        val l: N = childUpdated(node, 0, minValues, minValues, left)
        val m: A = middle
        val r: N = childUpdated(node, minValues, children, 0, right)
        (l, m, r)
      }
    }

    private def valueInserted(node: N, from: Int, to: Int, insertionPoint: Int, left: ChildNode, middle: A, right: ChildNode)(implicit builder: Builder): N = {
      val children = valueCount(node)
      val childIndex = insertionPoint + children
      builder.internal(to - from + 1)
      builder.copy(node, 1 + from, insertionPoint)
      builder.insertValue(middle)
      builder.copy(node, insertionPoint, 1 + to)
      builder.copy(node, 1 + from + children, childIndex)
      builder.insertChild(left)
      builder.insertChild(right)
      builder.copy(node, childIndex + 1, 1 + to + children + 1)
      builder.recalculateSize().result()
    }

    private def copied(node: N, from: Int, to: Int)(implicit builder: Builder): N = {
      val children = valueCount(node)
      builder.internal(to - from)
      builder.copy(node, 1 + from, 1 + to)
      builder.copy(node, 1 + from + children, 1 + to + children + 1)
      builder.recalculateSize().result()
    }

    private def childUpdated(node: N, from: Int, to: Int, childIndex: Int, child: ChildNode)(implicit builder: Builder): N = {
      val children = valueCount(node)
      builder.internal(to - from)
      builder.copy(node, 1 + from, 1 + to)
      builder.copy(node, 1 + from + children, 1 + to + children + 1)
      builder.updateChild(1 + to - from + childIndex, child)
      builder.recalculateSize().result()
    }

    override def valueAt(node: N, index: Int): A = node(index).asInstanceOf[A]
    override def childAt(node: N, index: Int): ChildNode = node(index).asInstanceOf[ChildNode]
    override def leftChild(node: N, index: Int): ChildNode = childAt(node, 1 + valueCount(node) + index - 1)
    override def rightChild(node: N, index: Int): ChildNode = childAt(node, 1 + valueCount(node) + index)

    override def deleteFromRoot(node: N, a: A)(implicit builder: Builder): Nullable[Either[N, ChildNode]] = {
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case null =>
          null
        case Left(merged) if children == 1 =>
          Right(merged)
        case Left(merged) if children > 1 =>
          Left(replaceMergedChildren(node, index, merged))
        case Right((left, middle, right)) =>
          Left(replaceUpdatedChildren(node, index, left, middle, right))
      }
    }
    def deleteAndMergeLeft(leftSibling: N, leftValue: A, node: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case null =>
          null
        case Left(merged) if children == minValues && valueCount(leftSibling) == minValues =>
          Left(replaceMergedChildAndMergeWithLeft(leftSibling, leftValue, node, index, merged))
        case Left(merged) if children > minValues =>
          Right((leftSibling, leftValue, replaceMergedChildren(node, index, merged)))
        case Left(merged) if valueCount(leftSibling) > minValues =>
          Right(replaceMergedChildAndTakeFromLeft(leftSibling, leftValue, node, index, merged))
        case Right((left, middle, right)) =>
          Right((leftSibling, leftValue, replaceUpdatedChildren(node, index, left, middle, right)))
      }
    }
    def deleteAndMergeRight(node: N, rightValue: A, rightSibling: N, a: A)(implicit builder: Builder): Nullable[Either[N, (N, A, N)]] = {
      val children = valueCount(node)
      val (index, result) = deleteValue(node, a)
      result match {
        case null =>
          null
        case Left(merged) if children == minValues && valueCount(rightSibling) == minValues =>
          Left(replaceMergedChildAndMergeWithRight(node, rightValue, rightSibling, index, merged))
        case Left(merged) if children > minValues =>
          Right((replaceMergedChildren(node, index, merged), rightValue, rightSibling))
        case Left(merged) if valueCount(rightSibling) > minValues =>
          Right(replaceMergedChildAndTakeFromRight(node, rightValue, rightSibling, index, merged))
        case Right((left, middle, right)) =>
          Right((replaceUpdatedChildren(node, index, left, middle, right), rightValue, rightSibling))
      }
    }
    override def rebalance(left: N, right: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      val leftCount = valueCount(left)
      val rightCount = valueCount(right)
      val highestLeftChild = childAt(left, 1 + leftCount + leftCount)
      val lowestRightChild = childAt(right, 1 + rightCount)
      childOps.rebalance(highestLeftChild, lowestRightChild)(builder.down) match {
        case Right((l, m, r)) =>
          builder.internal(leftCount)
          builder.copy(left, 1, left.length - 1)
          builder.insertChild(l)
          val updatedLeft = builder.recalculateSize().result()
          builder.internal(rightCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.insertChild(r)
          builder.copy(right, 1 + rightCount + 1, right.length)
          val updatedRight = builder.recalculateSize().result()
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
          val updatedLeft = builder.recalculateSize().result()
          val middle = valueAt(left, leftCount)
          builder.internal(rightCount)
          builder.copy(right, 1, 1 + rightCount)
          builder.insertChild(merged)
          builder.copy(right, 1 + rightCount + 1, right.length)
          val updatedRight = builder.recalculateSize().result()
          Right((updatedLeft, middle, updatedRight))
        case Left(merged) =>
          builder.internal(leftCount)
          builder.copy(left, 1, left.length - 1)
          builder.insertChild(merged)
          val updatedLeft = builder.recalculateSize().result()
          val middle = valueAt(right, 1)
          builder.internal(rightCount - 1)
          builder.copy(right, 2, 1 + rightCount)
          builder.copy(right, 1 + rightCount + 1, right.length)
          val updatedRight = builder.recalculateSize().result()
          Right((updatedLeft, middle, updatedRight))
      }
    }

    override def pathToHead(node: N, parent: Nullable[PathNode[A]]): Nullable[PathNode[A]] =
      childOps.pathToHead(leftChild(node, 1), new PathInternalNode(node, 0, parent))

    override def pathToKey(node: N, key: A, parent: Nullable[PathNode[A]]): Nullable[PathNode[A]] = {
      val children = valueCount(node)
      val index = search(node, key)
      if (index >= 0) new PathInternalNode(node, (index - 1) * 2 + 1, parent)
      else {
        val startPoint = -(index + 1)
        val child = childAt(node, startPoint + children)
        val parentPath = new PathInternalNode(node, (startPoint - 1) * 2, parent)
        val childPath = childOps.pathToKey(child, key, parentPath)
        if (childPath ne null) childPath else parentPath.next
      }
    }

    override def splitAtIndex(node: N, index: Int)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A]) = {
      val children = valueCount(node)
      var splitPoint = 1
      var position = index
      while (splitPoint <= children + 1) {
        val childIndex = splitPoint + children
        val child = childAt(node, childIndex)
        val childSize = childOps.size(child)
        if (position <= childSize) {
          val (leftChildSplit, rightChildSplit) = childOps.splitAtIndex(child, position)(builder.down)
          return mergeAfterSplit(node, splitPoint, leftChildSplit, rightChildSplit)
        } else {
          position -= childSize + 1
        }
        splitPoint += 1
      }
      throw new ArrayIndexOutOfBoundsException(index)
    }

    override def splitAtKey(node: N, key: A)(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A]) = {
      val children = valueCount(node)
      val index = search(node, key)
      val splitPoint = if (index >= 0) index else -(index + 1)
      val child = childAt(node, splitPoint + children)
      val (leftChildSplit, rightChildSplit) = childOps.splitAtKey(child, key)(builder.down)
      mergeAfterSplit(node, splitPoint, leftChildSplit, rightChildSplit)
    }

    private[this] def mergeAfterSplit(node: N, splitPoint: Int, leftChildSplit: NodeWithOps[A], rightChildSplit: NodeWithOps[A])(implicit builder: Builder): (NodeWithOps[A], NodeWithOps[A]) = {
      val children = valueCount(node)
      val leftSplit = if (splitPoint == 1) leftChildSplit else {
        val leftChild = childAt(node, splitPoint + children - 1)
        childOps.append(leftChild, valueAt(node, splitPoint - 1), leftChildSplit)(builder.down) match {
          case Left(merged) if splitPoint == 2 =>
            NodeWithOps(merged)
          case Left(merged) =>
            builder.internal(splitPoint - 2)
            builder.copy(node, 1, splitPoint - 1)
            builder.copy(node, 1 + children, children + splitPoint - 1)
            builder.insertChild(merged)
            NodeWithOps(builder.recalculateSize().result())
          case Right((left, middle, right)) =>
            builder.internal(splitPoint - 1)
            builder.copy(node, 1, splitPoint - 1)
            builder.insertValue(middle)
            builder.copy(node, 1 + children, children + splitPoint - 1)
            builder.insertChild(left)
            builder.insertChild(right)
            NodeWithOps(builder.recalculateSize().result())
        }
      }
      val rightSplit = if (splitPoint == 1 + children) rightChildSplit else {
        val rightChild = childAt(node, splitPoint + children + 1)
        childOps.prepend(rightChildSplit, valueAt(node, splitPoint), rightChild)(builder.down) match {
          case Left(merged) if splitPoint == children =>
            NodeWithOps(merged)
          case Left(merged) =>
            builder.internal(children - (splitPoint - 1) - 1)
            builder.copy(node, splitPoint + 1, children + 1)
            builder.insertChild(merged)
            builder.copy(node, children + splitPoint + 2, node.length)
            NodeWithOps(builder.recalculateSize().result())
          case Right((left, value, right)) =>
            builder.internal(children - (splitPoint - 1))
            builder.insertValue(value)
            builder.copy(node, splitPoint + 1, children + 1)
            builder.insertChild(left)
            builder.insertChild(right)
            builder.copy(node, children + splitPoint + 2, node.length)
            NodeWithOps(builder.recalculateSize().result())
        }
      }
      (leftSplit, rightSplit)
    }

    override def prepend(prefix: NodeWithOps[A], value: A, node: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      val children = valueCount(node)
      if (prefix.ops.level < level) {
        val leftMostChild = childAt(node, 1 + children)
        val updatedOrSplit = childOps.prepend(prefix, value, leftMostChild)(builder.down)
        childUpdatedOrSplit(node, 1, updatedOrSplit)
      } else if (prefix.ops.level == level) {
        val left = prefix.node.asInstanceOf[N]
        concatenate(left, value, node)
      } else {
        throw new AssertionError("prefix must be smaller")
      }
    }

    override def append(node: N, value: A, suffix: NodeWithOps[A])(implicit builder: Builder): Either[N, (N, A, N)] = {
      val children = valueCount(node)
      if (suffix.ops.level < level) {
        val rightMostChild = childAt(node, 1 + children + children)
        val updatedOrSplit = childOps.append(rightMostChild, value, suffix)(builder.down)
        childUpdatedOrSplit(node, 1 + children, updatedOrSplit)
      } else if (suffix.ops.level == level) {
        val right = suffix.node.asInstanceOf[N]
        concatenate(node, value, right)
      } else {
        throw new AssertionError("suffix must be smaller")
      }
    }

    override def head(node: N): A = childOps.head(childAt(node, 1 + valueCount(node)))
    override def last(node: N): A = childOps.last(childAt(node, node.length - 1))

    private[this] def concatenate(left: N, value: A, right: N)(implicit builder: Builder): Either[N, (N, A, N)] = {
      val leftCount = valueCount(left)
      val rightCount = valueCount(right)
      val totalCount = leftCount + 1 + rightCount
      if (totalCount <= maxValues) {
        Left(builder.internal(totalCount)
          .copy(left, 1, 1 + leftCount).insertValue(value).copy(right, 1, 1 + rightCount)
          .copy(left, 1 + leftCount, left.length).copy(right, 1 + rightCount, right.length)
          .recalculateSize().result())
      } else {
        val newLeftCount = totalCount / 2
        val newRightCount = totalCount - 1 - newLeftCount
        Right(if (newLeftCount < leftCount) {
          val l: N = builder.internal(newLeftCount)
            .copy(left, 1, 1 + newLeftCount)
            .copy(left, 1 + leftCount, 1 + leftCount + newLeftCount + 1)
            .recalculateSize().result()
          val m: A = valueAt(left, 1 + newLeftCount)
          val r: N = builder.internal(newRightCount)
            .copy(left, 1 + newLeftCount + 1, 1 + leftCount).insertValue(value).copy(right, 1, 1 + rightCount)
            .copy(left, 1 + leftCount + newLeftCount + 1, left.length).copy(right, 1 + rightCount, right.length)
            .recalculateSize().result()
          (l, m, r)
        } else if (newLeftCount > leftCount) {
          val l: N = builder.internal(newLeftCount)
            .copy(left, 1, 1 + leftCount).insertValue(value).copy(right, 1, 1 + newLeftCount - leftCount - 1)
            .copy(left, 1 + leftCount, left.length).copy(right, 1 + rightCount, 1 + rightCount + newLeftCount - leftCount)
            .recalculateSize().result()
          val m: A = valueAt(right, 1 + newLeftCount - leftCount - 1)
          val r: N = builder.internal(newRightCount)
            .copy(right, 1 + newLeftCount - leftCount, 1 + rightCount)
            .copy(right, 1 + rightCount + newLeftCount - leftCount, right.length)
            .recalculateSize().result()
          (l, m, r)
        } else {
          (left, value, right)
        })
      }
    }

    private def deleteValue(node: N, a: A)(implicit builder: Builder): (Int, Nullable[Either[ChildNode, (ChildNode, A, ChildNode)]]) = {
      val index = search(node, a)
      if (index >= 0) {
        (index, childOps.rebalance(leftChild(node, index), rightChild(node, index))(builder.down))
      } else {
        val insertionPoint = -index - 1
        val children = valueCount(node)
        val childIndex = insertionPoint + children
        val middle = childAt(node, childIndex)
        val left = if (childIndex > 1 + children) childAt(node, childIndex - 1) else null
        val right = if (childIndex + 1 < node.length) childAt(node, childIndex + 1) else null
        if (left == null || (right != null && childOps.valueCount(left) < childOps.valueCount(right))) {
          (insertionPoint, childOps.deleteAndMergeRight(middle, valueAt(node, insertionPoint), right, a)(builder.down))
        } else {
          (insertionPoint - 1, childOps.deleteAndMergeLeft(left, valueAt(node, insertionPoint - 1), middle, a)(builder.down))
        }
      }
    }

    private def replaceMergedChildren(node: N, insertionPoint: Int, merged: Node[L, A])(implicit builder: Builder): N = {
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
    private def replaceUpdatedChildren(node: N, insertionPoint: Int, left: Node[L, A], middle: A, right: Node[L, A])(implicit builder: Builder): N = {
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
    private def replaceMergedChildAndMergeWithLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: Builder): N = {
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
    private def replaceMergedChildAndMergeWithRight(node: N, rightValue: A, rightSibling: N, index: Int, merged: Node[L, A])(implicit builder: Builder): N = {
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
    private def replaceMergedChildAndTakeFromLeft(leftSibling: N, leftValue: A, node: N, index: Int, merged: Node[L, A])(implicit builder: Builder): (N, A, N) = {
      val leftCount = valueCount(leftSibling)
      builder.internal(leftCount - 1)
      builder.copy(leftSibling, 1, 1 + leftCount - 1)
      builder.copy(leftSibling, 1 + leftCount, leftSibling.length - 1)
      builder.recalculateSize()
      val updatedLeft = builder.result()
      val updatedMiddle = valueAt(leftSibling, leftCount)
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
    private def replaceMergedChildAndTakeFromRight(node: N, rightValue: A, rightSibling: N, index: Int, merged: Node[L, A])(implicit builder: Builder): (N, A, N) = {
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
      val updatedMiddle = valueAt(rightSibling, 1)
      val rightCount = valueCount(rightSibling)
      builder.internal(rightCount - 1)
      builder.copy(rightSibling, 2, 1 + rightCount)
      builder.copy(rightSibling, 1 + rightCount + 1, rightSibling.length)
      builder.recalculateSize()
      val updatedRight = builder.result()
      (updatedLeft, updatedMiddle, updatedRight)
    }

    private def search(node: N, a: A): Int = Arrays.binarySearch(node, 1, valueCount(node) + 1, a.asInstanceOf[AnyRef], valueComparator[A])
  }

  class Root[L <: Level, A](val root: Node[L, A])(implicit override val ordering: Ordering[A], val ops: NodeOps[L, A])
    extends BTreeSet[A] with Serializable {
    override def +(a: A) = {
      val builder = ops.newBuilder
      ops.insert(root, a, overwrite = false)(builder) match {
        case null       => this
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

    override def -(a: A): BTreeSet[A] = {
      ops.deleteFromRoot(root, a)(ops.newBuilder) match {
        case null          => this
        case Left(updated) => new Root(updated)
        case Right(child)  => new Root(child)(ops.ordering, ops.childOps)
      }
    }
    override def isEmpty: Boolean = ops.isEmpty(root)
    override def size: Int = ops.size(root)
    override def contains(a: A): Boolean = ops.contains(root, a)

    override def iterator: Iterator[A] = new BTreeIterator(root, ops.pathToHead(root))
    override def iteratorFrom(start: A): Iterator[A] = new BTreeIterator(root, ops.pathToKey(root, start))

    override def rangeImpl(from: Option[A], until: Option[A]): BTreeSet[A] = (from, until) match {
      case (None, None)      =>
        this
      case (Some(key), None) =>
        val nodeWithOps = ops.splitAtKey(root, key)(ops.newBuilder)._2
        new Root(nodeWithOps.node)(ordering, nodeWithOps.ops)
      case (None, Some(key)) =>
        val nodeWithOps = ops.splitAtKey(root, key)(ops.newBuilder)._1
        new Root(nodeWithOps.node)(ordering, nodeWithOps.ops)
      case (from, until)     =>
        rangeImpl(from, None).rangeImpl(None, until)
    }
    override def stringPrefix: String = "BTreeSet"

    // Optimized implementations of various methods.
    override def head = ops.head(root)
    override def headOption = if (isEmpty) None else Some(head)
    override def last = ops.last(root)
    override def lastOption = if (isEmpty) None else Some(last)
    override def tail = this - head
    override def init = this - last

    override def take(n: Int) =
      if (n <= 0) empty
      else if (n >= size) this
      else {
        val (left, _) = ops.splitAtIndex(root, n)(ops.newBuilder)
        new Root(left.node)(ordering, left.ops)
      }

    override def drop(n: Int) =
      if (n <= 0) this
      else if (n >= size) empty
      else {
        val (_, right) = ops.splitAtIndex(root, n)(ops.newBuilder)
        new Root(right.node)(ordering, right.ops)
      }

    override def splitAt(n: Int) =
      if (n <= 0) (empty, this)
      else if (n >= size) (this, empty)
      else {
        val (left, right) = ops.splitAtIndex(root, n)(ops.newBuilder)
        (new Root(left.node)(ordering, left.ops),
          new Root(right.node)(ordering, right.ops))
      }

    override def slice(from: Int, until: Int) = {
      if (from >= until || from >= size) empty
      else if (from <= 0) take(until)
      else if (until >= size) drop(from)
      else drop(from).take(until - from)
    }

    override def dropRight(n: Int) = take(size - n)
    override def takeRight(n: Int) = drop(size - n)

    private[this] def countWhile(p: A => Boolean): Int = {
      var result = 0
      val it = iterator
      while (it.hasNext && p(it.next())) result += 1
      result
    }

    override def dropWhile(p: A => Boolean) = drop(countWhile(p))
    override def takeWhile(p: A => Boolean) = take(countWhile(p))
    override def span(p: A => Boolean) = splitAt(countWhile(p))
  }

  sealed trait PathNode[A] {
    def current: A
    def next: PathNode[A]
  }

  private final class PathLeafNode[A](node: Node[Leaf, A], initialPosition: Int, parent: PathNode[A])(implicit ops: NodeOps[Leaf, A]) extends PathNode[A] {
    private[this] var position = initialPosition
    private[this] val valueCount = ops.valueCount(node)

    def current: A = ops.valueAt(node, position)

    def next: PathNode[A] =
      if (position + 1 < valueCount) {
        position += 1
        this
      } else if (parent ne null) {
        parent.next
      } else {
        null
      }
  }

  private final class PathInternalNode[L <: Next[_], A](node: Node[L, A], initialPosition: Int, parent: PathNode[A])(implicit ops: NodeOps[L, A]) extends PathNode[A] {
    private[this] var position = initialPosition
    private[this] val valueCount = ops.valueCount(node)
    private[this] val valueAndChildCount = 2 * valueCount + 1

    def current: A = ops.valueAt(node, 1 + ((position - 1) >> 1))

    def next: PathNode[A] =
      if (position + 1 < valueAndChildCount) {
        position += 1
        if ((position & 1) == 0)
          ops.childOps.pathToHead(ops.childAt(node, 1 + valueCount + (position >> 1)), this)
        else
          this
      } else if (parent ne null) {
        parent.next
      } else {
        null
      }
  }

  private final class BTreeIterator[L <: Level, A](root: Node[L, A], initialPath: PathNode[A] = null)(implicit ops: NodeOps[L, A]) extends Iterator[A] {
    private[this] var path: PathNode[A] = _
    private[this] var nextAvailable: Boolean = _
    private[this] var next: AnyRef = _

    tryNext(initialPath)

    override def hasNext: Boolean = nextAvailable
    override def next(): A = {
      if (!nextAvailable) throw new NoSuchElementException("next on empty iterator")
      val result = next.asInstanceOf[A]
      tryNext(path.next)
      result
    }

    private[this] def tryNext(nextPath: PathNode[A]): Unit = {
      path = nextPath
      nextAvailable = nextPath ne null
      next = if (nextAvailable) path.current.asInstanceOf[AnyRef] else null
    }
  }
}
