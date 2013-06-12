Immutable, in-memory B-Trees for Scala
======================================

Current status: proof-of-concept.

Performance is in the same ball-park as the standard Red-Black Tree
used by Scala's TreeSet implementation, but gets better when there are
many elements in the tree, due to higher memory efficiency.

Overall the B-Tree uses about 1/5th the memory of a Red-Black Tree
(assuming a minimum of 16 values in leaves).


Benchmarks
----------

Start sbt with something like:

```
$ SBT_OPTS="-Xms1G -Xmx1G -XX:+UseCompressedOops -XX:+UseSerialGC -XX:+TieredCompilation" sbt test:console
```

Then run a benchmark:

```
scala> scalax.collection.immutable.BTreeThyme.insertShuffled()
```

The benchmark make use of the excellent [Thyme](https://github.com/Ichoran/thyme) library.


Copyright
---------

Copyright (c) 2013 Zilverline B.V.  See
[LICENSE](https://github.com/zilverline/scala-btree/blob/master/LICENSE.md)
for details.
