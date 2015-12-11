Read Repair Notes
=================

DalmatinerDB makes use of read repair to ensure eventual consistency.  A client
performs a read from several nodes in parallel; and any stale values are
detected and replaced by a newer value on those replicas.

The quorum condition, r + w > n, allows the system to tolerate unavailable
nodes. The book "Designing Data Intensive Applications" by Martin Kleppmann
(O'Reilly) lists the following situations in which stale values may be returned:

* When a write occurs concurrently with a read, it cannot be determined
  whether the old or new value will be returned in the read.

* A write that succeeds on some replicas, but fails on others will not be
  rolled back in the case that a write quorum is not met. 
  [Bringing consistency to Riak, Joseph Blomstedt - https://vimeo.com/51973001]

* A node carrying a new value may be restored from another replica carrying a
  stale value. This may break the quorum condition, as the number of nodes
  carrying the new value could fall below w.

* Complex sequences of faults may lead to conditions in which the quorum
  condition is broken.
  [Absolute consistency, Joseph Blomstedt -
  http://lists.basho.com/pipermail/riak-users_lists.basho.com/2012-January/007157.html]

* In the case of Sloppy Quorum, writes may no longer overlap with reads, as
  writes may be performed on replicas that are different to those being read
  from.

Forcing Read Repair
------------------

In a cluster of N nodes, N and W may both be set to low values, while R can be
set to a high value, even N itself.  This forces requests to be performed
against all nodes, causing stale/non-existent values to be read.

Finally, Basho notes that read repair may be forced by increasing the n_val,
together with an increasing R value, may cause the failed read operations.
[https://docs.basho.com/riak/1.3.1/references/appendices/concepts/Replication]
