# Declarative pipelines using the Disruptor

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

In this post we'll recall the problem of using "normal" queues, discuss how
Disruptor helps solve this problem and have a look at how we can we provide a
declarative high-level language for expressing pipelines backed by Disruptors
where all low-level details are hidden away from the user of the library. We'll
also have a look at how we can monitor and visualise such pipelines for
debugging and performance troubleshooting purposes.

## Motivation and inspiration

Before we dive into *how* we can achieve this, let's start with the question of
*why* I'd like to do it.

I believe the way we write programs for multi processors, i.e. multiple
computers each with multiple CPUs, can be improved upon. Instead of focusing on
the pitfalls of the current mainstream approaches to these problems, let's have
a look at what to me seems like the most promising way forward.

Jim Gray gave a great explaination of dataflow programming in this Turing Award
Recipient [interview](https://www.youtube.com/watch?v=U3eo49nVxcA&t=1949s). He
uses props to make his point, which makes it a bit difficult to summaries in
text here. I highly recommend watching the video clip, the relevant part is only
3 minutes long.

The key point is exactly that of pipelining. Each stage is running on a
CPU/core, this program is completely sequential, but by connecting several
stages we create a parallel pipeline. Further parallelism (what Jim calls
partitioned parallelism) can be gained by partitioning the inputs, by say odd
and even sequence number, and feeding one half of the inputs to one copy of the
pipeline and the other half to another copy, thereby almost doubling the
throughput. Jim calls this a "natural" way to achieve parallelism.

While I'm not sure if "natural" is the best word, I do agree that it's a nice
way to make good use of CPUs/cores on a single computer without introducing
non-determinism.

Things get a bit more tricky if we want to involve more computers. Part of the
reason, I believe, is that we run into the problem highlighted by Barbara Liskov
at the very end of her Turing award
[lecture](https://youtu.be/qAKrMdUycb8?t=3058) (2009):

> "There's a funny disconnect in how we write distributed programs. You
>  write your individual modules, but then when you want to connect
>  them together you're out of the programming language and into this
>  other world. Maybe we need languages that are a little bit more
>  complete now, so that we can write the whole thing in the language."

Ideally we'd like our pipelines to seemlessly span over multiple computers.

In fact it should be possible to deploy same pipeline to different
configurations of processors without changing the pipeline code.

A pipeline that is redeployed with additional CPUs or computers might or might
not scale, it depends on whether it makes sense to partition the input of a
stage further or if perhaps the introduction of an additonal computer merely
adds more overhead.

How exactly the pipeline is best spread over the available computers and
CPUs/cores will require some combination of domain knowledge, measurement and
judgement.

Depending on how quick we can make redeploying of pipelines, it might be
possilbe to autoscale them using a program that monitors the queue lengths.

Also related to redeploying, but even more important than autoscaling, are
upgrades of pipelines.

Both upgrading the code running at the individual stages, but also the pipeline
itself.


More recently, Martin Thompson has given many talks which echo the general ideas
of Jim and Barbara. Martin also coauthored the [reactive
manifesto](https://www.reactivemanifesto.org/) which captures many of these
ideas in text. Martin is also one of the people behind the Disruptor, which we
will come back to soon, and he also [said](https://youtu.be/OqsAGFExFgQ?t=2532)
the following:

> "If there's one thing I'd say to the Erlang folks, it's you got the stuff right
> from a high-level, but you need to invest in your messaging infrastructure so
> it's super fast, super efficient and obeys all the right properties to let this
> stuff work really well."

which together with Joe Armstrong's
[anecdote](https://youtu.be/bo5WL5IQAd0?t=2494) of an unmodified Erlang program
*only* running 33 times faster on a 64 core machine has made me think about how
one can improve upon the already excellent work that Erlang is doing in this
space.

Longer term, I like to think of pipelines spanning computers as a building block
for what Barbara [calls](https://www.youtube.com/watch?v=8M0wTX6EOVI) a
"substrate for distributed systems". Unlike Barbara I don't think this substrate
should be based on shared memory, but overall I agree with her goal of making it
easier to program distributed systems by providing generic building blocks.

## Prior work

Working with streams of data is common.

Many streaming libraries are not:

  1. doing parallel processing (or if so, they don't do it deterministically or
     without copying data)
  2. cannot span multiple computers

Don't have a good deploy, upgrade, rescale story.

### Haskell

* Pipes / Conduit
* streamly

### Scala

* Akka streams and Akka cluster
* Spark streaming

### Clojure

* transducers

### Java

* Disruptor wizard
* Aeron

### Dataflow

* Lustre / SCADA / Esterel
* https://en.wikipedia.org/wiki/Dataflow_programming

### (Functional) reactive programming

* https://hackage.haskell.org/package/dunai-0.11.2/docs/Data-MonadicStreamFunction-Parallel.html
* [Parallel Functional Reactive Programming](http://flint.cs.yale.edu/trifonov/papers/pfrp.pdf) by Peterson et al. (2000)

* http://conal.net/papers/push-pull-frp/push-pull-frp.pdf
> "Peterson et al. (2000) explored opportunities for parallelism in
> implementing a variation of FRP. While the underlying semantic
> model was not spelled out, it seems that semantic determinacy was
> not preserved, in contrast to the semantically determinate concur-
> rency used in this paper (Section 11)."

* Where "determinate" is defined in
  http://conal.net/papers/warren-burton/Indeterminate%20behavior%20with%20determinate%20semantics%20in%20parallel%20programs.pdf

## Plan

The rest of this post is organised as follows.

First we'll have a look at how to model pipelines as a transformation of lists.
The purpose of this is to give us an easy to understand sequential specification
of we would like our pipelines to do.

We'll then give our first parallel implementation of pipelines using "normal"
queues. The main point here is to recap of the problem that arises from using
"normal" queues, but we'll also sketch how one can test the parallel
implementation using the model.

After that we'll have a look at the Disruptor API, sketch its single producer
implementation and discuss how it helps solve the problems we identified in the
previous section.

Finally we'll have enough background to be able to sketch the Disruptor
implementation of pipelines. We'll also discuss how monitoring/observability can
be added.

## List transformer model

Let's first introduce the type for our pipelines. We index our pipeline datatype
by two types, in order to be able to precisely specify its input and output
types. For example, the `Id`entity pipeline has the same input as output type,
while pipeline composition (`:>>>`) expects its first argument to be a pipeline
from `a` to `b`, and the second argument a pipeline from `b` to `c` in order for
the resulting composed pipeline to be from `a` to `c` (similar to functional
composition).

```haskell
data P :: Type -> Type -> Type where
  Id      :: P a a
  (:>>>)  :: P a b -> P b c -> P a c
  Map     :: (a -> b) -> P a b
  (:***)  :: P a c -> P b d -> P (a, b) (c, d)
  (:&&&)  :: P a b -> P a c -> P a (b, c)
  (:+++)  :: P a c -> P b d -> P (Either a b) (Either c d)
  (:|||)  :: P a c -> P b c -> P (Either a b) c
```

Here's a pipeline that takes a stream of integers as input and outputs a stream
of pairs where the first component is the input integer and the second component
is a boolean indicating if the first component was an even integer or not.

```haskell
examplePipeline :: P Int (Int, Bool)
examplePipeline = Id :&&& Map even
```

So far our pipelines are merely data which describes what we'd like to do. In
order to actually perform a stream transformation we'd need to give semantics to
our pipeline datatype.

The simplest semantics we can give our pipelines is that in terms of list
transformations.

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
    -- Note that we pass in the input list, in order to perserve the order.
    merge es (model f xs) (model g ys)
  where
    merge []             []       []       = []
    merge (Left  _ : es) (l : ls) rs       = Left  l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = Right r : merge es ls rs
model (f :||| g) es =
  let
    (xs, ys) = partitionEithers es
  in
    merge es (model f xs) (model g ys)
  where
    merge []             []       []       = []
    merge (Left  _ : es) (l : ls) rs       = l : merge es ls rs
    merge (Right _ : es) ls       (r : rs) = r : merge es ls rs
```

Note that this semantics is completely sequential and perserves the order of the
inputs (determinism). We'll introduce parallelism without breaking determinism
in the next section.

We can now run our example pipeline in the REPL:

```
> model examplePipeline [1,2,3,4,5]
[(1,False),(2,True),(3,False),(4,True),(5,False)]
```

Side note: the design space of what pipeline combinators to include in the
pipeline datatype is very big. I've chosen the ones I've done because they are
instances of already well established type classes:

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

Ideally we'd also like to be able to use `Arrow` notation/syntax to descripe our
pipelines[^1].

## Queue pipeline deployment

In the previous section we saw how to deploy pipelines in a purely sequential
way in order to process lists. The purpose of this is merely to give ourselves
an intuition of what pipelines should do as well as an executable model which we
can test our intuition against.

Next we shall have a look at our first parallel deployment. The idea here is to
show how we can involve multiple threads in the stream processing, without
making the output non-deterministic (same input should always give the same
output).

We can achieve this as follows:

```haskell
deploy :: P a b -> TQueue a -> IO (TQueue b)
deploy Id         xs = return xs
deploy (f :>>> g) xs = deploy g =<< deploy f xs
deploy (Map f)    xs = do
  ys <- newTQueueIO
  forkIO $ forever $ do
    x <- atomically (readTQueue xs)
    let y = f x
    atomically (writeTQueue ys y)
  return ys
deploy (f :&&& g) xs = do
  xs1 <- newTQueueIO
  xs2 <- newTQueueIO
  forkIO $ forever $ do
    x <- atomically (readTQueue xs)
    atomically $ do
      writeTQueue xs1 x
      writeTQueue xs2 x
  ys <- deploy f xs1
  zs <- deploy g xs2
  yzs <- newTQueueIO
  forkIO $ forever $ do
    y <- atomically (readTQueue ys)
    z <- atomically (readTQueue zs)
    atomically (writeTQueue yzs (y, z))
  return yzs
```

```haskell
example' :: [Int] -> IO [(Int, Bool)]
example' xs0 = do
  xs <- newTQueueIO
  mapM_ (atomically . writeTQueue xs) xs0
  ys <- deploy (Id :&&& Map even) xs
  replicateM (length xs0) (atomically (readTQueue ys))
```

Running this in our REPL, gives the same result as in the model:

```
> example' [1,2,3,4,5]
[(1,False),(2,True),(3,False),(4,True),(5,False)]
```

In fact, we can use our model to define a property-based test which asserts that
our queue deployement is faithful to the model:

```haskell
prop_commute :: Eq b => P a b -> [a] -> PropertyM IO ()
prop_commute p xs = do
  ys <- run $ do
    qxs <- newTQueueIO
    mapM_ (atomically . writeTQueue qxs) xs
    qys <- deploy p qxs
    replicateM (length xs) (atomically (readTQueue qys))
  assert (model p xs == ys)
```

Actually running this property for arbitary pipelines would require us to first
define a pipeline generator, which is a bit tricky given the indexes of the
datatype[^2]. It can still me used as a helper for testing specific pipelines
though, e.g. `prop_commute examplePipeline`.

A bigger problem is that we've spawned two threads, when deploying `:&&&`, whose
mere job is to copy elements from the input queue (`xs`) to the input queues of
`f` and `g` (`xs{1,2}`), and from the outputs of `f` and `g` (`ys` and `zs`) to
the output of `f &&& g` (`ysz`).

Copying data is expensive and we also lost two threads in the process.

This is pretty much where we left off in my previous post.

We also had a look at how to shard inputs (for partitioned parallelism) as well
made a simple benchmark to illustrate the speed ups we can get from pipelining.

We'll not repeat those things here.

## Disruptor

Before we can understand how the Disruptor can help us avoid the problem copying
between queues that we just saw, we need to first understand a bit about how the
Disruptor is implemented.

We will be looking at the implementation of the single-producer Disruptor,
because in our pipelines there will never be more than one producer per queue
(the stage before it)[^3].

Let's first have a look at the datatype and then explain each field:

```haskell
data RingBuffer a = RingBuffer
  { capacity             :: Int
  , elements             :: IOArray Int a
  , cursor               :: IORef SequenceNumber
  , gatingSequences      :: IORef (IOArray Int (IORef SequenceNumber))
  , cachedGatingSequence :: IORef SequenceNumber
  }

newtype SequenceNumber = SequenceNumber Int
```

The Disruptor is a ring buffer queue with a fixed `capacity`. It's backed by an
array whose length is equal to the capacity, this is where the `elements` of the
ring buffer are stored. There's a monotonically increasing counter called the
`cursor` which keeps track of how many elements we have written. By taking the
value of the `cursor` modulo the `capacity` we get the index into the array
where we are supposed to write our next element (this is how we wrap around the
array, i.e. forming a ring). In order to avoid overwriting elements which have
not yet been consumed we also need to keep track of the cursors of all consumers
(`gatingSeqeunces`). As an optimisation we cache where the last consumer is
(`cachedGatingSequence`).

The API from the producing side looks as follows:

```haskell
tryClaimBatch   :: RingBuffer a -> Int -> IO (Maybe SequenceNumber)
writeRingBuffer :: RingBuffer a -> SequenceNumber -> a -> IO ()
publish         :: RingBuffer a -> SequenceNumber -> IO ()
```

We first try to claim `n :: Int` slots in the ring buffer, if that fails
(returns `Nothing`) then we know that there isn't space in the ring buffer and
we should apply backpressure upstream (e.g. if the producer is a webserver, we
might want to temporarily rejecting clients with status code 503). Once we
successfully get a sequence number, we can start writing our data. Finally we
publish the sequence number, this makes it available on the consumer side.

The consumer side of the API looks as follows:

```haskell
addGatingSequence :: RingBuffer a -> IO (IORef SequenceNumber)
waitFor           :: RingBuffer a -> SequenceNumber -> IO SequenceNumber
readRingBuffer    :: RingBuffer a -> SequenceNumber -> IO a
```

First we need to add a consumer to the ring buffer (to avoid overwriting on wrap
around of the ring), this gives us a consumer cursor. The consumer is
responsible for updating this cursor, the ring buffer will only read from it to
avoid overwriting. After the consumer reads the cursor, it calls `waitFor` on
the read value, this will block until an element has been `publish`ed on that
slot by the producer. In the case that the producer is ahead it will return the
current sequence number of the producer, hence allowing the consumer to do a
batch of reads (from where it currently is to where the producer currently is).
Once the consumer has caught up with the producer it updates its cursor.

Here's an example which hopefully makes things more concrete:

```haskell
example :: IO ()
example = do
  rb <- newRingBuffer_ 2
  c <- addGatingSequence rb
  let batchSize = 2
  Just hi <- tryClaimBatch rb batchSize
  let lo = hi - (coerce batchSize - 1)
  assertIO (lo == 0)
  assertIO (hi == 1)
  -- Notice that these writes are batched:
  mapM_ (\(i, c) -> writeRingBuffer rb i c) (zip [lo..hi] ['a'..])
  publish rb hi
  -- Since the ring buffer size is only two and we've written two
  -- elements, it's full at this point:
  Nothing <- tryClaimBatch rb 1
  consumed <- readIORef c
  produced <- waitFor rb consumed
  -- The consumer can do batched reads, and only do some expensive
  -- operation once it reaches the end of the batch:
  xs <- mapM (readRingBuffer rb) [consumed + 1..produced]
  assertIO (xs == "ab")
  -- The consumer updates its cursor:
  writeIORef c produced
  -- Now there's space again for the producer:
  Just 2 <- tryClaimBatch rb 1
  return ()
```

See the `Disruptor` [module](src/Disruptor.hs) in case you are interested in the
implementation details.

## Disruptor pipeline deployment

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
* Hot-code upgrades of workers/stages with zero downtim
* Shard/scale/reroute pipelines and add more machines without downtime
  - auto scaling thread pools, https://github.com/stevana/elastically-scalable-thread-pools
* Generator for pipelines
* Benchmarks

## See also

* Guy Steele's talk [How to Think about Parallel Programming:
  Not!](https://www.infoq.com/presentations/Thinking-Parallel-Programming/)

* Mike Barker's [bruteforce solution to Guy's problem and
  benchmarks](https://github.com/mikeb01/folklore/tree/master/src/main/java/performance)

* [Understanding the Disruptor, a Beginner's Guide to Hardcore
  Concurrency](https://youtube.com/watch?v=DCdGlxBbKU4) by Trisha Gee and Mike
  Barker (2011)

* https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/

* [*SEDA: An Architecture for Well-Conditioned Scalable Internet
  Services*](https://people.eecs.berkeley.edu/~brewer/papers/SEDA-sosp.pdf)


[^1]: Even better would be if arrow notation worked for Cartesian categories.
    See Conal Elliott's work on [compiling to
    categories](http://conal.net/papers/compiling-to-categories/) , as well as
    Oleg Grenrus' GHC
    [plugin](https://github.com/phadej/overloaded/blob/master/src/Overloaded/Categories.hs)
    that does the right thing and translates arrow syntax into Cartesian
    categories.

[^2]: Search for "QuickCheck GADTs" if you are interested in finding out more
    about this topic.

[^3]: The Disruptor also comes in a multi-producer variant, see the following
    [repository](https://github.com/stevana/pipelined-state-machines/tree/main/src/Disruptor/MP)
    for a Haskell version or the
    [LMAX](https://github.com/LMAX-Exchange/disruptor) repository for the
    original Java implementation.
