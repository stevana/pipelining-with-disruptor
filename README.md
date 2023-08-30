# An Arrow-based EDSL for Disruptor pipelines

*Work in progress, please don't share yet*

In a previous
[post](https://github.com/stevana/pipelined-state-machines#pipelined-state-machines)
I explored how we can make better use of our parallel hardware by means of
pipelining.

In a nutshell the idea of pipelining is to break up the problem in stages and
have one (or more) thread(s) per stage and then connect the stages with queues.
For example, imagine a service where we read some request from a socket, parse
it, validate, update our state and construct a respose, serialise the response
and send it back over the socket. These are six distinct stages and we could
create a pipeline with six CPUs/cores each working on a their own stage and
feeding the output to the queue of the next stage. If one stage is slow we can
shard the input, e.g. even requests to go to one worker and odd requests to to
another thereby nearly doubling the throughput for that stage.

One of the conclusing remarks to the previous post is that we can gain even more
performace by using a better implementation of queues, e.g. the [LMAX
Disruptor](https://en.wikipedia.org/wiki/Disruptor_(software)).

The Disruptor is a low-latency high-throughput queue implementation with support
for multi-cast (many consumers can in parallel process the same event), batching
(both on producer and consumer side), back-pressure, sharding (for scalability)
and dependencies between consumers.

In this post we'll recall the problem of using "simple" queues, discuss how
Disruptor helps solve this problem and have a look at how we can we provide a
declarative high-level language for expressing pipelines backed by Disruptors
where all low-level details are hidden away from the user of the library. We'll
also have a look at how we can monitor and visualise such pipelines for
debugging and performance troubleshooting purposes.

## Motivation

Before we dive into *how* we can achieve this, let's start with the question of
*why* I'd like to do it.

I believe the way we write programs for multi processors, i.e. multiple
computers each with multiple CPUs, can be improved upon. Instead of focusing on
the pitfalls of the current mainstream approaches to these problems, let's have
a look at what to me seems like the most promising ways forward.

Jim Gray gave a great explaination of dataflow programming in this Turing Award
Recipient [interview](https://www.youtube.com/watch?v=U3eo49nVxcA&t=1949s). He
uses props to make his point, which makes it difficult to summaries in text
here. I highly recommend watching the video clip, the relevant part is only 3
minutes long.

The key point is exactly that of pipelining. Each stage is running on a
CPU/core, this program is completely sequential, but by connecting several
stages we create a parallel pipeline. Further parallelism (what Jim calls
partitioned parallelism) can be gained by partitioning the inputs, by say odd
and even sequence number, and feeding one half of the inputs to one copy of the
pipeline and the other half to another copy, thereby almost doubling the
throughput.

This is a nice model of making good use of CPUs/cores on a single computer. As
we involve more computers we run into the problem highlighted by Barbara Liskov
at the very end of her Turing award
[lecture](https://youtu.be/qAKrMdUycb8?t=3058) (2009):

> "There's a funny disconnect in how we write distributed programs. You
>  write your individual modules, but then when you want to connect
>  them together you're out of the programming language and into this
>  other world. Maybe we need languages that are a little bit more
>  complete now, so that we can write the whole thing in the language."

Ideally we'd like our pipelines to seemlessly span over multiple computers.

In fact it should be possible to deploy same pipeline to different
configurations of processors without changing the pipeline.

A pipeline that is deployed with more CPUs or more computers should, with minimal change, scale almost linearly.
  - auto scaling thread pools, https://github.com/stevana/elastically-scalable-thread-pools

* Martin Thompson, reactive manifesto, disruptor, erlang messaging comment?

- Erlang
  33x faster on 64 core machine, without changing the software at all
  https://youtu.be/bo5WL5IQAd0?t=2494

- Substrate https://www.youtube.com/watch?v=8M0wTX6EOVI


1. Break problem up in stages
2. Implement each stage
3. Connect stages together into a pipeline
4. Deploy pipeline on one or over several machines
5. Update implementations of stages with zero downtime
6. Monitor/observe pipelines to spot bottlenecks
7. Shard/scale/reroute pipelines and add more machines without downtime
8. Determinstic while parallel

## Prior work

### Haskell

* Pipes / Conduit
* streamly

### Scala

* Akka streams and Akka cluster
* Spark streaming

## Plan

The rest of this post is organised as follows.

First we will have a look at a model or specification of what we'd like our
pipelines to do.

This model is sequential and


* sequential list transformer model
* parallel queue implementation + problems
* disruptor standalone
* disruptor pipelines
* monitoring

## List transformer model

```haskell
data P a b where
  Id      :: P a a
  (:>>>)  :: P a b -> P b c -> P a c
  Map     :: (a -> b) -> P a b
  (:***)  :: P a c -> P b d -> P (a, b) (c, d)
  (:&&&)  :: P a b -> P a c -> P a (b, c)
  (:+++)  :: P a c -> P b d -> P (Either a b) (Either c d)
  (:|||)  :: P a c -> P b c -> P (Either a b) c
```


```haskell
model :: P a b -> [a] -> [b]
model Id         xs  = xs
model (f :>>> g) xs  = model g (model f xs)
model (Map f)    xs  = map f xs
model (f :*** g) xys =
  let
    (xs, ys) = unzip xys
  in
    zip (model f xs) (model g ys)
model (f :&&& g) xs = zip (model f xs) (model g xs)
model (f :+++ g) es =
  let
    (xs, ys) = partitionEithers es
  in
    merge es (model f xs) (model g ys)
  where
    merge (Left  _ : es) (l : ls) rs       = Left  l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = Right r : merge es ls rs
model (f :||| g) es =
  let
    (xs, ys) = partitionEithers es
  in
    merge es (model f xs) (model g ys)
  where
    merge (Left  _ : es) (l : ls) rs       = l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = r : merge es ls rs
```

```haskell
instance Category P where
  id    = Id
  g . f = f :>>> g

instance Arrow P where
  arr     = Map
  f *** g = f :*** g
  f &&& g = f :&&& g

instance ArrowChoice P where
  f +++ g = f :+++ g
  f ||| g = f :||| g
```

## Queue deployment (first parallel deployment)

```
 [a] -- l --> [b] ------- return -------> IO [b]
  |                                       |
  | toQ                                   | IO id
  v                                       v
Q a -- q --> IO (Q b) -- fmap toList --> IO [b]
```

```haskell
prop_commute :: P a b -> [a] -> PropertyM IO Bool
prop_commute p xs = do
  ys <- run (atomically flushTQueue <$> deploy p (toQ xs))
  assert (model p xs == ys)
```

## Disruptor

In the previous post we used simple FIFO queues (`TBQueue`s to be precise), one
of the problems with those is that if we want to, for example, fan out one event
to several processors we first need to copy the event to the processors queues.

```
              +--------> [] [ processor 1]
             /
  [ev1, ev2]
```



This copy is one of many things that makes the Disruptor a better queue
implementation choice.


## Arrow EDSL

## Deploying

## Example

## Monitoring

## Running

XXX: update to use cabal:
```bash
ghc -O2 src/Pipeline.hs -o ./hs-wc -threaded -prof -fprof-auto -rtsopts

cat data/test.txt | ./hs-wc +RTS -p -N

ghc-prof-flamegraph hs-wc.prof
firefox hs-wc.svg
```

## Further work

* Avoid writing NoOutput
* Actual Arrow instance
* Can we be smarter about Either?
* More monitoring?
* Deploy across network of computers
* Hot-code upgrades of workers

## See also

* Guy Steele's talk [How to Think about Parallel Programming:
  Not!](https://www.infoq.com/presentations/Thinking-Parallel-Programming/)

* Mike Barker's [bruteforce solution to Guy's problem and
  benchmarks](https://github.com/mikeb01/folklore/tree/master/src/main/java/performance)

* https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
