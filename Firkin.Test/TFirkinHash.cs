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
using log4net;
using NUnit.Framework;
using Droog.Firkin.Util;

namespace Droog.Firkin.Test {

    [TestFixture]
    public class TFirkinHash {

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private FirkinHash<string> _hash;
        private string _path;

        [SetUp]
        public void Setup() {
            _path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        public void CreateHash() {
            _hash = new FirkinHash<string>(_path);
        }

        [TearDown]
        public void Teardown() {
            _hash.Dispose();
            Directory.Delete(_path, true);
        }

        [Test]
        public void Can_read_write_entry() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value, stream2.To<string>());
        }

        [Test]
        public void Can_read_write_entry_with_hash_reload() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value, stream2.To<string>());
        }

        [Test]
        public void Can_read_write_multiple_with_hash_reload() {
            CreateHash();
            var data = Enumerable.Range(1, 5);
            foreach(var v in data) {
                var stream = v.ToStream();
                _hash.Put("k" + v, stream, stream.Length);
            }
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            foreach(var v in data) {
                var stream2 = _hash.Get("k" + v);
                Assert.AreEqual(v, stream2.To<int>());
            }
        }

        [Test]
        public void Can_delete_record() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Delete(key);
            Assert.IsNull(_hash.Get(key));
        }

        [Test]
        public void Delete_persists_after_hash_reload() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Delete(key);
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            Assert.IsNull(_hash.Get(key));
        }

        [Test]
        public void Can_overwrite_record() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var value2 = "baz";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            stream = value2.ToStream();
            _hash.Put(key, stream, stream.Length);
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value2, stream2.To<string>());
        }

        [Test]
        public void Overwrite_persists_after_hash_reload() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var value2 = "baz";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            stream = value2.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value2, stream2.To<string>());
        }

        [Test]
        public void Can_delete_and_write_record_again() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var value2 = "baz";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Delete(key);
            stream = value2.ToStream();
            _hash.Put(key, stream, stream.Length);
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value2, stream2.To<string>());
        }

        [Test]
        public void Can_delete_and_write_record_again_after_hash_reload() {
            CreateHash();
            var key = "foo";
            var value = "bar";
            var value2 = "baz";
            var stream = value.ToStream();
            _hash.Put(key, stream, stream.Length);
            _hash.Delete(key);
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            stream = value2.ToStream();
            _hash.Put(key, stream, stream.Length);
            var stream2 = _hash.Get(key);
            Assert.AreEqual(value2, stream2.To<string>());
        }

        [Test]
        public void Active_rolls_over_at_size_barrier() {
            _hash = new FirkinHash<string>(_path, 30);
            var stream = "bar".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            stream.Position = 0;
            _hash.Put("foo2", stream, stream.Length);
            stream.Position = 0;
            _hash.Put("foo3", stream, stream.Length);
            stream.Position = 0;
            Assert.AreEqual(4, Directory.GetFiles(_path).Count());
        }

        [Test]
        public void Can_access_keys_across_files_after_hash_reload() {
            _hash = new FirkinHash<string>(_path, 30);
            var stream = "bar1".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            stream = "bar2".ToStream();
            _hash.Put("foo2", stream, stream.Length);
            stream = "bar3".ToStream();
            _hash.Put("foo3", stream, stream.Length);
            _hash.Dispose();
            _hash = new FirkinHash<string>(_path, 30);
            Assert.AreEqual("bar3", _hash.Get("foo3").To<string>());
            Assert.AreEqual("bar1", _hash.Get("foo1").To<string>());
            Assert.AreEqual("bar2", _hash.Get("foo2").To<string>());
        }

        [Test]
        public void Can_call_merge_and_retrieve_data() {
            _hash = new FirkinHash<string>(_path, 60);
            var stream = "bar1".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            stream = "bar2".ToStream();
            _hash.Put("foo2", stream, stream.Length);
            stream = "bar3".ToStream();
            _hash.Put("foo3", stream, stream.Length);
            stream = "bar4".ToStream();
            _hash.Put("foo4", stream, stream.Length);
            stream = "bar1x".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            _hash.Merge();
            Assert.AreEqual(4, _hash.Count);
            Assert.AreEqual("bar3", _hash.Get("foo3").To<string>());
            Assert.AreEqual("bar1x", _hash.Get("foo1").To<string>());
            Assert.AreEqual("bar2", _hash.Get("foo2").To<string>());
            Assert.AreEqual("bar4", _hash.Get("foo4").To<string>());
        }

        [Test]
        public void Can_call_merge_and_enumerate_data() {
            _hash = new FirkinHash<string>(_path, 60);
            var stream = "bar1".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            stream = "bar2".ToStream();
            _hash.Put("foo2", stream, stream.Length);
            stream = "bar3".ToStream();
            _hash.Put("foo3", stream, stream.Length);
            stream = "bar4".ToStream();
            _hash.Put("foo4", stream, stream.Length);
            stream = "bar1x".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            _hash.Merge();
            Assert.AreEqual(new[] { "bar1x", "bar2", "bar3", "bar4" }, (from item in _hash orderby item.Key select item.Value.To<string>()).ToArray());
            Assert.AreEqual(new[] { "bar1x", "bar2", "bar3", "bar4" }, (from item in _hash orderby item.Key select item.Value.To<string>()).ToArray());
        }

        [Test]
        public void Can_call_merge_and_reload_hash_then_retrieve_data() {
            _hash = new FirkinHash<string>(_path, 30);
            var stream = "bar1".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            stream = "bar2".ToStream();
            _hash.Put("foo2", stream, stream.Length);
            stream = "bar3".ToStream();
            _hash.Put("foo3", stream, stream.Length);
            stream = "bar4".ToStream();
            _hash.Put("foo4", stream, stream.Length);
            stream = "bar1x".ToStream();
            _hash.Put("foo1", stream, stream.Length);
            _hash.Merge();
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            _hash = new FirkinHash<string>(_path, 30);
            Assert.AreEqual("bar3", _hash.Get("foo3").To<string>());
            Assert.AreEqual("bar1x", _hash.Get("foo1").To<string>());
            Assert.AreEqual("bar2", _hash.Get("foo2").To<string>());
            Assert.AreEqual("bar4", _hash.Get("foo4").To<string>());
        }

        [Test]
        public void Writing_empty_stream_is_a_delete() {
            CreateHash();
            _hash.Put("foo", new MemoryStream(), 0);
            Assert.AreEqual(0, _hash.Count);
            Assert.IsNull(_hash.Get("foo"));
        }

        [Test]
        public void Read_write_delete_consistency() {
            var r = new Random(1234);
            CreateHash();
            var dictionary = new Dictionary<string, byte[]>();
            for(var i = 0; i < 1000; i++) {
                var k = "k" + r.Next(100);
                if(r.Next(4) == 3) {
                    dictionary.Remove(k);
                    _hash.Delete(k);
                } else {
                    var v = TestUtil.GetRandomBytes(r);
                    dictionary[k] = v;
                    _hash.Put(k, v.ToStream(), v.Length);
                }
                _hash.Get("k" + r.Next(100));
            }
            Assert.AreEqual(dictionary.Count, _hash.Count);
            foreach(var pair in dictionary) {
                Assert.AreEqual(0, pair.Value.Compare(_hash.Get(pair.Key).ReadBytes()));
            }
        }

        [Test]
        public void Read_write_delete_consistency_with_reload_before_read() {
            var r = new Random(1234);
            CreateHash();
            var dictionary = new Dictionary<string, byte[]>();
            for(var i = 0; i < 1000; i++) {
                var k = "k" + r.Next(100);
                if(r.Next(4) == 3) {
                    dictionary.Remove(k);
                    _hash.Delete(k);
                } else {
                    var v = TestUtil.GetRandomBytes(r);
                    dictionary[k] = v;
                    _hash.Put(k, v.ToStream(), v.Length);
                }
                _hash.Get("k" + r.Next(100));
            }
            _hash.Dispose();
            _log.DebugFormat("re-loading hash");
            CreateHash();
            Assert.AreEqual(dictionary.Count, _hash.Count);
            foreach(var pair in dictionary) {
                Assert.AreEqual(0, pair.Value.Compare(_hash.Get(pair.Key).ReadBytes()));
            }
        }

        [Test]
        public void Read_write_delete_consistency_with_merge_before_read() {
            var r = new Random(1234);
            _hash = new FirkinHash<string>(_path, 10 * 1024);
            var dictionary = new Dictionary<string, byte[]>();
            for(var i = 0; i < 200; i++) {
                var k = "k" + r.Next(100);
                if(r.Next(4) == 3) {
                    dictionary.Remove(k);
                    _hash.Delete(k);
                } else {
                    var v = TestUtil.GetRandomBytes(r);
                    dictionary[k] = v;
                    _hash.Put(k, v.ToStream(), v.Length);
                }
                _hash.Get("k" + r.Next(100));
            }
            _hash.Merge();
            Assert.AreEqual(dictionary.Count, _hash.Count);
            foreach(var pair in dictionary) {
                Assert.AreEqual(0, pair.Value.Compare(_hash.Get(pair.Key).ReadBytes()));
            }
        }

        [Test]
        public void Read_write_delete_consistency_with_merge_in_middle() {
            var r = new Random(1234);
            _hash = new FirkinHash<string>(_path, 10 * 1024);
            var dictionary = new Dictionary<string, byte[]>();
            for(var i = 0; i < 500; i++) {
                var k = "k" + r.Next(100);
                if(r.Next(4) == 3) {
                    dictionary.Remove(k);
                    _hash.Delete(k);
                } else {
                    var v = TestUtil.GetRandomBytes(r);
                    dictionary[k] = v;
                    _hash.Put(k, v.ToStream(), v.Length);
                }
                _hash.Get("k" + r.Next(100));
            }
            _hash.Merge();
            for(var i = 0; i < 500; i++) {
                var k = "k" + r.Next(100);
                if(r.Next(5) == 5) {
                    dictionary.Remove(k);
                    _hash.Delete(k);
                } else {
                    var v = TestUtil.GetRandomBytes(r);
                    dictionary[k] = v;
                    _hash.Put(k, v.ToStream(), v.Length);
                }
                _hash.Get("k" + r.Next(100));
            }
            Assert.AreEqual(dictionary.Count, _hash.Count);
            foreach(var pair in dictionary) {
                Assert.AreEqual(0, pair.Value.Compare(_hash.Get(pair.Key).ReadBytes()));
            }
        }


        [Test]
        public void Read_write_delete_consistency_with_multiple_merges() {
            var r = new Random(1234);
            _hash = new FirkinHash<string>(_path, 10 * 2048);
            var dictionary = new Dictionary<string, byte[]>();
            for(var j = 0; j < 10; j++) {
                for(var i = 0; i < 1000; i++) {
                    var k = "k" + r.Next(100);
                    if(r.Next(4) == 3) {
                        dictionary.Remove(k);
                        _hash.Delete(k);
                    } else {
                        var v = TestUtil.GetRandomBytes(r);
                        dictionary[k] = v;
                        _hash.Put(k, v.ToStream(), v.Length);
                    }
                    _hash.Get("k" + r.Next(100));
                }
                var skip = true;
                foreach(var k in _hash) {
                    skip = !skip;
                    if(skip) {
                        continue;
                    }
                    dictionary.Remove(k.Key);
                    _hash.Delete(k.Key);
                }
                _hash.Merge();
            }
            Assert.AreEqual(dictionary.Count, _hash.Count);
            foreach(var pair in dictionary) {
                Assert.AreEqual(0, pair.Value.Compare(_hash.Get(pair.Key).ReadBytes()));
            }
        }

        [Test]
        public void Can_truncate_hash() {
            var r = new Random(1234);
            CreateHash();
            for(var i = 0; i < 1000; i++) {
                var k = "k" + i;
                var v = TestUtil.GetRandomBytes(r);
                _hash.Put(k, v.ToStream(), v.Length);
            }
            Assert.AreEqual(1000, _hash.Count);
            _hash.Truncate();
            Assert.AreEqual(0, _hash.Count);
        }

        [Test]
        public void Can_truncate_hash_and_stays_truncated_after_reload() {
            var r = new Random(1234);
            CreateHash();
            for(var i = 0; i < 1000; i++) {
                var k = "k" + i;
                var v = TestUtil.GetRandomBytes(r);
                _hash.Put(k, v.ToStream(), v.Length);
            }
            Assert.AreEqual(1000, _hash.Count);
            _hash.Truncate();
            _hash.Dispose();
            CreateHash();
            Assert.AreEqual(0, _hash.Count);
        }
    }
}
