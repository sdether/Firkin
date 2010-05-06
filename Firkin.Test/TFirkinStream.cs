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
using System.IO;
using Droog.Firkin.IO;
using NUnit.Framework;
using Droog.Firkin.Util;

namespace Droog.Firkin.Test {

    [TestFixture]
    public class TFirkinStream {

        [Test]
        public void FirkinStream_memorizes_small_streams_automatically() {
            var stream = CreateStream(1000);
            var fs = new FirkinStream(new StreamSyncRoot(), stream, 0, 1000);
            Assert.IsTrue(fs.IsMemorized);
        }

        [Test]
        public void FirkinStream_does_not_automatically_memorize_large_streams() {
            var stream = CreateStream(100000);
            var fs = new FirkinStream(new StreamSyncRoot(), stream, 1000, 90000);
            Assert.IsFalse(fs.IsMemorized);
        }

        [Test]
        public void Can_memorize_large_stream() {
            var stream = CreateStream(100000);
            var length = 90000;
            var fs = new FirkinStream(new StreamSyncRoot(), stream, 1000, length);
            fs.Memorize();
            Assert.IsTrue(fs.IsMemorized);
            var bytes = fs.ReadBytes();
            Assert.AreEqual(length,bytes.Length);
            stream.Position = 1000;
            Assert.AreEqual(0, stream.ReadBytes(length).Compare(bytes));
        }

        [Test]
        public void Can_read_memorized_stream_in_small_chunks() {
            var stream = CreateStream(1000);
            var length = 500;
            var fs = new FirkinStream(new StreamSyncRoot(), stream, 100, (uint)length);
            var read = -1;
            var total = 0;
            var buffer = new byte[length];

            while(read != 0) {
                read = fs.Read(buffer, total, 100);
                total += read;
            }
            Assert.AreEqual(length, total);
            stream.Position = 100;
            Assert.AreEqual(0, stream.ReadBytes(length).Compare(buffer));
        }
        [Test]
        public void Can_read_memorized_stream_in_single_chunk() {
            var stream = CreateStream(1000);
            var length = 500;
            var fs = new FirkinStream(new StreamSyncRoot(), stream, 100, (uint)length);
            var buffer = new byte[length];
            Assert.AreEqual(length, fs.Read(buffer, 0, length));
            stream.Position = 100;
            Assert.AreEqual(0, stream.ReadBytes(length).Compare(buffer));
        }

        private MemoryStream CreateStream(int size) {
            var stream = new MemoryStream();
            var r = new Random();
            for(var i = 0; i < size; i++) {
                stream.WriteByte((byte)(21 + r.Next(120)));
            }
            return stream;
        }
    }
}
