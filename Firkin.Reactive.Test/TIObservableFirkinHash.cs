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
using Droog.Firkin;
using Droog.Firkin.Test;
using log4net;
using NUnit.Framework;

namespace Firkin.Reactive.Test {

    [TestFixture]
    public class TIObservableFirkinHash {
        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private ObservableFirkinHash<string> _observable;
        private string _path;

        [SetUp]
        public void Setup() {
            _path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        public ObservableFirkinHash<string> CreateObservable() {
            return _observable = new ObservableFirkinHash<string>(_path);
        }

        [TearDown]
        public void Teardown() {
            _observable.Dispose();
            Directory.Delete(_path, true);
        }

        [Test]
        public void Can_receive_sequence_of_adds() {
            var observable = CreateObservable();
            var changes = new List<FirkinHashChange<string>>();
            observable.Subscribe(changes.Add);
            observable.Put("foo", 1.ToStream());
            observable.Put("bar", 2.ToStream());
            observable.Put("baz", 3.ToStream());
            Assert.AreEqual(new[] { "foo", "bar", "baz" }, changes.Select(x => x.Key).ToArray());
            Assert.AreEqual(new[] { FirkinHashChangeAction.Add, FirkinHashChangeAction.Add, FirkinHashChangeAction.Add }, changes.Select(x => x.Action).ToArray());
        }

        [Test]
        public void Can_receive_sequence_of_ACDs() {
            var observable = CreateObservable();
            var changes = new List<FirkinHashChange<string>>();
            observable.Subscribe(changes.Add);
            observable.Put("foo", 1.ToStream());
            observable.Put("foo", 2.ToStream());
            observable.Delete("foo");
            Assert.AreEqual(new[] { "foo", "foo", "foo" }, changes.Select(x => x.Key).ToArray());
            Assert.AreEqual(new[] { FirkinHashChangeAction.Add, FirkinHashChangeAction.Change, FirkinHashChangeAction.Delete }, changes.Select(x => x.Action).ToArray());
        }

        [Test]
        public void Sequence_terminates_when_hash_is_disposed() {
            var observable = CreateObservable();
            var done = new ManualResetEvent(false);
            var next = new ManualResetEvent(false);
            var error = new ManualResetEvent(false);
            var observer = Observer.Create<FirkinHashChange<string>>(x => next.Set(), x => error.Set(), () => done.Set());
            observable.Subscribe(observer);
            Thread.Sleep(100);
            Assert.IsFalse(done.WaitOne(100));
            Assert.IsFalse(next.WaitOne(100));
            Assert.IsFalse(error.WaitOne(100));
            observable.Dispose();
            Assert.IsTrue(done.WaitOne(100));
            Assert.IsFalse(next.WaitOne(100));
            Assert.IsFalse(error.WaitOne(100));
        }
    }
}
