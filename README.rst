Firkin 0.2
==========
An embeddable Key/Value store for .NET and mono using immutable log journalling and in-memory hashing as its storage back end. Inspired by the Basho BitCask paper located at http://downloads.basho.com/papers/bitcask-intro.pdf. Supports IObservable interface from the Rx framework with Firkin.Reactive, allowing changes to be tailed.

Uses
====
- Quickly store and retrieve binary data by unique key

Installation
============
Currently using the driver in the GAC is not supported.  Simply copy the driver assembly somewhere and reference it in your project.  It should be deployed in your application's bin directory.  It is not necessary to reference the test assemblies.

Dependencies
============
Firkin.Reactive, Firkin.Reactive.Test and Firkin.Test.Perf require the Rx Framework to be installed (http://msdn.microsoft.com/en-us/devlabs/ee794896.aspx). Firkin itself does not require Rx.

Patches
=======
Patches are welcome and will likely be accepted.  By submitting a patch you assign the copyright to me, Arne F. Claassen.  This is necessary to simplify the number of copyright holders should it become necessary that the copyright need to be re-assigned or the code re-licensed.  The code will always be available under an OSI approved license.

Roadmap
=======
- Create ``FirkinIndex`` to create secondary indicies into a ``FirkinHash``
- Add size (total and active) to ``FirkinHash`` to provide a metric to base merge use on
- Put ``FirkinHash`` through some proper benchmarking and concurrency testing

Usage
=====

Usage of base store, ``FirkinHash<TKey>``

::

  // create a new store
  var store = new FirkinHash<string>(storageDirectory);

  // store a value
  store.Put(key, valueStream, valueStreamLength);
  
  // iterate over all files (won't block other reads or writes)
  foreach(var pair in store) {
    var key = pair.Key;
    var valueStream = pair.Value;
  }

  // get a value
  var valueStream = store.Get(key);

  // remove a value
  var removed = store.Delete(key);

  // get store size
  var count = store.Count;

  // merge log files to remove overwritten and deleted entries
  store.Merge(); // does not block reads or writes (mostly)
  
Or use ``FirkinDictionary<TKey,TValue>`` like any ``IDictionary``

::

  // create new disk bound dictionary
  var dictionary = new FirkinDictionary<string,string>(storageDirectory);

Some unscientific perf data
===========================
The write/random query test of all users from the StackOverflow dump included in the project has shown the following single threaded numbers:

  41k writes/second
  
  80k queries/second
  
Using ObservableFirkinHash currently adds a ~20% perf penalty for writes, since it checks the index to determine whether the action is a add or change.


Contributors
============
- Arne F. Claassen (sdether)


