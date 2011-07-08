/*
 * Firkin 
 * Copyright (C) 2010 Arne F. Claassen
 * http://www.claassen.net/geek/blog geekblog [at] claassen [dot] net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using log4net;
using log4net.Config;
using NUnit.Framework;

namespace Droog.Firkin.Test {

    [TestFixture]
    public class TFirkinDictionary {

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private FirkinDictionary<int, string> _dictionary;
        private string _path;

        [SetUp]
        public void Setup() {
            _path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        public void CreateDictionary() {
            _dictionary = new FirkinDictionary<int, string>(_path);
        }

        [TearDown]
        public void Teardown() {
            if(_dictionary != null) {
                _dictionary.Dispose();
            }
            if(Directory.Exists(_path)) {
                Directory.Delete(_path, true);
            }
        }

        [Test]
        public void Can_store_retrieve_items_in_dictionary() {
            CreateDictionary();
            _dictionary.Add(1234, "foobar");
            Assert.AreEqual("foobar", _dictionary[1234]);
        }

        [Test]
        public void Can_store_retrieve_items_in_dictionary_with_reload() {
            CreateDictionary();
            _dictionary.Add(1234, "foobar");
            _dictionary.Dispose();
            CreateDictionary();
            Assert.AreEqual("foobar", _dictionary[1234]);
        }

        [Test]
        public void Can_clear_dictionary() {
            CreateDictionary();
            for(int i = 0; i < 1000; i++) {
                _dictionary.Add(i, "v" + i);
            }
            Assert.AreEqual(1000, _dictionary.Count);
            _dictionary.Clear();
            Assert.AreEqual(0, _dictionary.Count);
        }

        [Test]
        public void Can_iterate_over_keyvalue_pairs() {
            CreateDictionary();
            var dictionary = new Dictionary<int, string>();
            for(int i = 0; i < 100; i++) {
                _dictionary.Add(i, "v" + i);
                dictionary.Add(i, "v" + i);
            }

            foreach(var kvp in _dictionary) {
                Assert.AreEqual(dictionary[kvp.Key], kvp.Value);
                dictionary.Remove(kvp.Key);
            }
            Assert.AreEqual(0, dictionary.Count);
        }

        [Test]
        public void Can_iterate_over_keys() {
            CreateDictionary();
            var dictionary = new Dictionary<int, string>();
            for(int i = 0; i < 100; i++) {
                _dictionary.Add(i, "v" + i);
                dictionary.Add(i, "v" + i);
            }

            foreach(var key in _dictionary.Keys) {
                var value = _dictionary[key];
                Assert.AreEqual(dictionary[key], value);
                dictionary.Remove(key);
            }
            Assert.AreEqual(0, dictionary.Count);
        }

        [Test]
        public void Can_iterator_over_value() {
            CreateDictionary();
            var hashSet = new HashSet<string>();
            for(int i = 0; i < 100; i++) {
                _dictionary.Add(i, "v" + i);
                hashSet.Add("v" + i);
            }

            foreach(var value in _dictionary.Values) {
                Assert.IsTrue(hashSet.Contains(value));
                hashSet.Remove(value);
            }
            Assert.AreEqual(0, hashSet.Count);
        }

        [Test]
        public void Cache_scenario_write_consistency_with_multiple_merges() {
            var r = new Random(1234);
            var keys = new Queue<string>();
            AddItems(keys, 200);
            using(var d = new FirkinDictionary<string, string>(
                 _path,
                 1024 * 1024,
                 Serialization.SerializerRepository.GetByteArraySerializer<string>(),
                 Serialization.SerializerRepository.GetStreamSerializer<string>()
           )) {
                var dictionary = new Dictionary<string, string>();
                var n = 0;
                var t = 0;
                while(keys.Any()) {
                    n++;
                    t++;
                    var key = keys.Dequeue();
                    var v = TestUtil.GetRandomString(r);
                    dictionary[key] = v;
                    if(d.ContainsKey(key)) {
                        var x = d[key];
                    }
                    d[key] = v;
                    switch(r.Next(10)) {
                        case 1:
                            keys.Enqueue(key);
                            break;
                        case 4:
                            AddItems(keys, 10);
                            break;
                    }
                    if(n >= 3000) {
                        d.Merge();
                        n = 0;
                    }
                    if(t >= 20000) {
                        break;
                    }
                }
                foreach(var file in Directory.GetFiles(_path)) {
                    _log.DebugFormat(Path.GetFileName(file));
                }
                _log.DebugFormat("total items {0} after {1} iterations with {2} left in queue", d.Count, t, keys.Count);
                Assert.AreEqual(dictionary.Count, d.Count);
                foreach(var pair in dictionary) {
                    Assert.AreEqual(pair.Value, d[pair.Key]);
                }
            }
        }

        [Test, Explicit]
        public void Run_till_crash_with_parallel_merge() {
            var r = new Random(1234);
            uint serial = 0;
            var keys = new Queue<string>();
            AddItems(keys, 1000);
            uint last = 0;
            var mergeCounter = 0;
            var inMerge = false;
            using(var d = new FirkinDictionary<string, uint>(_path)) {
                new Thread(() => {
                    while(true) {
                        if(mergeCounter > 100000) {
                            inMerge = true;
                            Interlocked.Exchange(ref mergeCounter, 0);
                            var s = serial;
                            Console.WriteLine("{0} iterations, merge time", s);
                            var limit = s - 20000;
                            var remove = (from entry in d where entry.Value < limit select entry.Key).ToArray();
                            Console.WriteLine("will remove {0} of {1}", remove.Length, d.Count);
                            foreach(var key in remove) {
                                d.Remove(key);
                            }
                            var preMerge = d.Count;
                            d.Merge();
                            var postMerge = d.Count;
                            inMerge = false;
                            Console.WriteLine("pre: {0} / post: {1}", preMerge, postMerge);
                            foreach(var file in Directory.GetFiles(_path)) {
                                Console.WriteLine(Path.GetFileName(file));
                            }
                        }
                        Thread.Sleep(1000);
                    }
                }) { IsBackground = true }.Start();
                while(true) {
                    Interlocked.Increment(ref mergeCounter);
                    var nextKey = keys.Dequeue();
                    if(d.ContainsKey(nextKey)) {
                        last = d[nextKey];
                    }
                    d[nextKey] = ++serial;
                    keys.Enqueue(r.Next(10) == 1 ? nextKey : Guid.NewGuid().ToString());
                    if(inMerge) {
                        Thread.Sleep(10);
                    }
                }
            }
        }

        [Test, Explicit]
        public void Run_till_crash_with_parallel_writes() {
            BasicConfigurator.Configure();
            var r = new Random(1234);
            int serial = 0;
            var keys = new Queue<string>();
            AddItems(keys, 1000);
            uint last = 0;
            var mergeCounter = 0;
            var inMerge = false;
            var faults = new List<Exception>();
            using(var d = new FirkinDictionary<string, uint>(_path)) {
                for(var i = 0; i < 10; i++) {
                    var workerId = i;
                    var worker = new Thread(() => {
                        try {
                            _log.DebugFormat("worker {0} started", workerId);
                            while(true) {
                                Interlocked.Increment(ref mergeCounter);
                                string nextKey;
                                lock(keys) {
                                    nextKey = keys.Dequeue();
                                }
                                try {
                                    if(d.ContainsKey(nextKey)) {
                                        last = d[nextKey];
                                    }
                                } catch(ObjectDisposedException) { }
                                var v = (uint)Interlocked.Increment(ref serial);
                                d[nextKey] = v;
                                lock(keys) {
                                    keys.Enqueue(r.Next(10) == 1 ? nextKey : Guid.NewGuid().ToString());
                                }
                                if(inMerge) {
                                    Thread.Sleep(1000);
                                }
                            }
                        } catch(Exception e) {
                            Console.WriteLine("Worker {0} failed: {1}\r\n{2}", workerId, e.Message, e);
                            faults.Add(e);
                        }
                    }) { IsBackground = true };
                    worker.Start();
                }
                while(true) {
                    if(faults.Any()) {
                        throw faults.First();
                    }
                    if(mergeCounter > 100000) {
                        try {
                            inMerge = true;
                            Interlocked.Exchange(ref mergeCounter, 0);
                            var s = serial;
                            Console.WriteLine("{0} iterations, merge time", s);
                            var limit = s - 20000;
                            var remove = (from entry in d where entry.Value < limit select entry.Key).ToArray();
                            Console.WriteLine("will remove {0} of {1}", remove.Length, d.Count);
                            foreach(var key in remove) {
                                d.Remove(key);
                            }
                            var preMerge = d.Count;
                            d.Merge();
                            var postMerge = d.Count;
                            inMerge = false;
                            Console.WriteLine("pre: {0} / post: {1}", preMerge, postMerge);
                            foreach(var file in Directory.GetFiles(_path)) {
                                Console.WriteLine(Path.GetFileName(file));
                            }
                        } catch(Exception e) {
                            Console.WriteLine("merger failed: {0}\r\n{1}", e.Message, e);
                            throw e;
                        }
                    }
                    Thread.Sleep(1000);
                }
            }
        }

        [Test, Explicit]
        public void Run_till_crash_with_serial_merge() {
            var r = new Random(1234);
            uint serial = 0;
            var keys = new Queue<string>();
            AddItems(keys, 1000);
            uint last = 0;
            var mergeCounter = 0;
            using(var d = new FirkinDictionary<string, uint>(_path)) {
                while(true) {
                    Interlocked.Increment(ref mergeCounter);
                    var nextKey = keys.Dequeue();
                    if(d.ContainsKey(nextKey)) {
                        last = d[nextKey];
                    }
                    d[nextKey] = ++serial;
                    keys.Enqueue(r.Next(10) == 1 ? nextKey : Guid.NewGuid().ToString());
                    if(mergeCounter > 100000) {
                        Interlocked.Exchange(ref mergeCounter, 0);
                        var s = serial;
                        Console.WriteLine("{0} iterations, merge time", s);
                        var limit = s - 20000;
                        var remove = (from entry in d where entry.Value < limit select entry.Key).ToArray();
                        Console.WriteLine("will remove {0} of {1}", remove.Length, d.Count);
                        foreach(var key in remove) {
                            d.Remove(key);
                        }
                        var preMerge = d.Count;
                        d.Merge();
                        var postMerge = d.Count;
                        Console.WriteLine("pre: {0} / post: {1}", preMerge, postMerge);
                        foreach(var file in Directory.GetFiles(_path)) {
                            Console.WriteLine(Path.GetFileName(file));
                        }
                    }
                }
            }
        }

        [Test, Explicit]
        public void Memory_consumption() {
            var r = new Random(1234);
            var keys = new Queue<string>();
            AddItems(keys, 200);
            var baseline = GC.GetTotalMemory(true);
            string capture = "";
            using(var d = new FirkinDictionary<string, string>(_path)) {
                var n = 0;
                var t = 0;
                var m = 0;
                while(keys.Any()) {
                    n++;
                    t++;
                    var key = keys.Dequeue();
                    var v = TestUtil.GetRandomString(r);
                    if(d.ContainsKey(key)) {
                        var x = d[key];
                        capture = ".." + x;
                    }
                    d[key] = v;
                    switch(r.Next(10)) {
                        case 1:
                            keys.Enqueue(key);
                            break;
                        case 4:
                            if(keys.Count < 200) {
                                AddItems(keys, 10);
                            }
                            break;
                    }
                    if(n >= 5000) {
                        m++;
                        var before = GC.GetTotalMemory(true);
                        d.Merge();
                        var after = GC.GetTotalMemory(true);
                        var c = d.Count;
                        Console.WriteLine(
                            "merge {0}, iteration {1}, items: {2}, before {3:0.00}MB, after {4:0.00}MB, storage {5:0.00}bytes/item)",
                            m,
                            t,
                            c,
                            (before - baseline) / 1024 / 1024,
                            (after - baseline) / 1024 / 1024,
                            (after - baseline) / c
                            );
                        n = 0;
                    }
                    if(t >= 200000) {
                        break;
                    }
                    if(keys.Count < 50) {
                        AddItems(keys, 100);
                    }
                }
                Console.WriteLine("total items {0} after {1} iterations with {2} left in queue", d.Count, t, keys.Count);
                _log.Debug(capture.Substring(0, 10));
            }
        }

        [Test, Explicit]
        public void Memory_consumption_parallel_writes_and_merges() {
            var r = new Random(1234);
            var keys = new Queue<string>();
            AddItems(keys, 200);
            var baseline = GC.GetTotalMemory(true);
            using(var d = new FirkinDictionary<string, string>(_path)) {
                var t = 0;
                var capture = "";
                var done = false;
                var n = 0;
                var merger = new Thread(() => {
                    var m = 0;
                    while(!done) {
                        if(n >= 5000) {
                            m++;
                            var before = GC.GetTotalMemory(true);
                            Console.WriteLine(
                                "merge {0}, before {1:0.00}MB)",
                                m,
                                (before - baseline) / 1024 / 1024
                            );
                            var expiredKeys = (from entry in d
                                               where entry.Value.Length != 0 && r.Next(4) == 1
                                               select entry.Key).ToArray();
                            foreach(var key in expiredKeys) {
                                d.Remove(key);
                            }
                            var during = GC.GetTotalMemory(true);
                            Console.WriteLine(
                                "merge {0}, during {1:0.00}MB)",
                                m,
                                (during - baseline) / 1024 / 1024
                            );
                            d.Merge();

                            var after = GC.GetTotalMemory(true);
                            var c = d.Count;
                            Console.WriteLine(
                                "merge {0}, iteration {1}, items: {2}, after {3:0.00}MB, storage {4:0.00}bytes/item)",
                                m,
                                t,
                                c,
                                (after - baseline) / 1024 / 1024,
                                (after - baseline) / c
                            );
                            n = 0;
                        }
                    }
                }) { IsBackground = true };
                merger.Start();
                while(keys.Any()) {
                    n++;
                    t++;
                    var key = keys.Dequeue();
                    var v = TestUtil.GetRandomString(r);
                    if(d.ContainsKey(key)) {
                        var x = d[key];
                        capture = ".." + x;
                    }
                    d[key] = v;
                    switch(r.Next(10)) {
                        case 1:
                            keys.Enqueue(key);
                            break;
                        case 4:
                            if(keys.Count < 200) {
                                AddItems(keys, 10);
                            }
                            break;
                    }
                    if(t >= 1000000) {
                        break;
                    }
                    if(keys.Count < 50) {
                        AddItems(keys, 100);
                    }
                }
                done = true;
                merger.Join();
                Console.WriteLine("total items {0} after {1} iterations with {2} left in queue", d.Count, t, keys.Count);
                _log.Debug(capture.Substring(0, 10));
            }
        }

        private void AddItems(Queue<string> keys, int n) {
            for(var i = 0; i < n; i++) {
                keys.Enqueue(Guid.NewGuid().ToString());
            }
        }
    }
}
