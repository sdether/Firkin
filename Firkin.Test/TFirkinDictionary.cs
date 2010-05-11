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
using NUnit.Framework;

namespace Droog.Firkin.Test {

    [TestFixture]
    public class TFirkinDictionary {

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
            _dictionary.Dispose();
            Directory.Delete(_path, true);
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
                Assert.AreEqual(dictionary[kvp.Key],kvp.Value);
                dictionary.Remove(kvp.Key);
            }
            Assert.AreEqual(0,dictionary.Count);
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
    }
}
