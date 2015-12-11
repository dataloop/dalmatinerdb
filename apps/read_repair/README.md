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

Test Cases
==========

The test data is purposefully simple in order to easily verify correctness.
Assume Node 1 is Claimant C, and the key for the test metric always hashes to 
a VNode partition residing on Node 1.
Assume the force remove operation involves no handoffs.

3 time series are available, all of size |Cache Points (CP)|
  * continuous: [ x | x <- [1..CP] ]
  * odds:       [ x | x <- [1..CP] x rem 2 <> 0 ]
  * evens:      [ x | x <- [1..CP] x rem 2 = 0 ]

Case 1
--------------------------------------
Repair to non-existent VNode cache

Join nodes to cluster, N = 1, W = 1, R = 4
Write (Node 1, T, odd)
Read  (Node 1, T)
Force remove node 4, bring node up independently
Read (Node 4, T) must equal odd

Before read (replica replies):
```
{{0,'dev1@127.0.0.1'},
{1000, <<1,0,0,0,0,0,0,1,1,0,0,0,0,0,0,3,1,0,0,0,0,0,0,5,...>>}}

{{182687704666362864775460604089535377456991567872,'dev2@127.0.0.2'},
{1000,<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0...>>}}

{{365375409332725729550921208179070754913983135744,'dev3@127.0.0.3'},
{1000,<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0...>>}}

{{548063113999088594326381812268606132370974703616,'dev4@127.0.0.4'},
{1000,<<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0...>>}}
```

After read (replica replies):
```
{{0,'dev1@127.0.0.1'},
{1000, <<1,0,0,0,0,0,0,1,1,0,0,0,0,0,0,3,1,0,0,0,0,0,0,5,...>>}}

{{182687704666362864775460604089535377456991567872,'dev2@127.0.0.2'},
{1000, <<1,0,0,0,0,0,0,1,1,0,0,0,0,0,0,3,1,0,0,0,0,0,0,5,...>>}}

{{365375409332725729550921208179070754913983135744,'dev3@127.0.0.3'},
{1000, <<1,0,0,0,0,0,0,1,1,0,0,0,0,0,0,3,1,0,0,0,0,0,0,5,...>>}}

{{548063113999088594326381812268606132370974703616,'dev4@127.0.0.4'},
{1000, <<1,0,0,0,0,0,0,1,1,0,0,0,0,0,0,3,1,0,0,0,0,0,0,5,...>>}}
```

Case 2
-------------------------------------
Repair falls ahead of VNode cache

No cluster
Write (Node 1, T, odd)
Write (Node 2, T, even)
Write (Node 3, T, odd)
Write (Node 4, T, even)

Join nodes to cluster, N = 1, W = 1, R = 4
Set T' = T + CP + 1
Write (Node 1, T', even)
Read  (Node 1, T')
Force remove node 3, bring node up independently
Read (Node 3, T) must equal odd
Read (Node 3, T') must equal even

Case 3
-------------------------------------
Repair falls behind the VNode cache

No cluster
Write (Node 1, T, odd)
Write (Node 2, T, even)
Write (Node 3, T, odd)
Write (Node 4, T, even)

Join nodes to cluster, N = 1, W = 1, R = 4
Set T' = T - CP - 1
Write (Node 1, T', even)
Read  (Node 1, T')
Force remove node 3, bring node up independently
Read (Node 3, T) must equal odd
Read (Node 3, T') must equal even

Case 4
-------------------------------------
Repair overlaps the cache

No cluster
Write (Node 1, T, odd)
Write (Node 2, T, even)
Write (Node 3, T, odd)
Write (Node 4, T, even)

Join nodes to cluster, N = 1, W = 1, R = 4
Write (Node 1, T, even)
Read  (Node 1, T)
Force remove node 3, bring node up independently
Read (Node 3, T) must equal continuous, fully interleaved sequence
