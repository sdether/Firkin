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
using Droog.Firkin.Serialization;
using NUnit.Framework;

namespace Droog.Firkin.Test {
    
    [TestFixture]
    public class TSerialization {

        [Test]
        public void Can_roundtrip_short_as_stream() {
            RoundTripStream<short>(42);
        }

        [Test]
        public void Can_roundtrip_short_as_bytearray() {
            RoundtripByteArray<short>(42);
        }

        [Test]
        public void Can_roundtrip_ushort_as_stream() {
            RoundTripStream<ushort>(42);
        }

        [Test]
        public void Can_roundtrip_ushort_as_bytearray() {
            RoundtripByteArray<ushort>(42);
        }

        [Test]
        public void Can_roundtrip_int_as_stream() {
            RoundTripStream<int>(42);
        }

        [Test]
        public void Can_roundtrip_int_as_bytearray() {
            RoundtripByteArray<int>(42);
        }

        [Test]
        public void Can_roundtrip_uint_as_stream() {
            RoundTripStream<uint>(42);
        }

        [Test]
        public void Can_roundtrip_uint_as_bytearray() {
            RoundtripByteArray<uint>(42);
        }

        [Test]
        public void Can_roundtrip_long_as_stream() {
            RoundTripStream<long>(42);
        }

        [Test]
        public void Can_roundtrip_long_as_bytearray() {
            RoundtripByteArray<long>(42);
        }

        [Test]
        public void Can_roundtrip_ulong_as_stream() {
            RoundTripStream<ulong>(42);
        }

        [Test]
        public void Can_roundtrip_ulong_as_bytearray() {
            RoundtripByteArray<ulong>(42);
        }

        [Test]
        public void Can_roundtrip_float_as_stream() {
            RoundTripStream<float>(42);
        }

        [Test]
        public void Can_roundtrip_float_as_bytearray() {
            RoundtripByteArray<float>(42);
        }

        [Test]
        public void Can_roundtrip_double_as_stream() {
            RoundTripStream<double>(42);
        }

        [Test]
        public void Can_roundtrip_double_as_bytearray() {
            RoundtripByteArray<double>(42);
        }

        [Test]
        public void Can_roundtrip_string_as_stream() {
            RoundTripStream("fortytwo");
        }

        [Test]
        public void Can_roundtrip_string_as_bytearray() {
            RoundtripByteArray("fortytwo");
        }

        [Test]
        public void Can_roundtrip_serializable_as_stream() {
            RoundTripStream(new Serializable() {Id = 42, Name = "fortywo"});
        }

        [Test]
        public void Can_roundtrip_serializable_as_bytearray() {
            RoundtripByteArray(new Serializable() { Id = 42, Name = "fortywo" });
        }

        private void RoundtripByteArray<T>(T value) {
            var serializer = SerializerRepository.GetByteArraySerializer<T>();
            Assert.AreEqual(value, serializer.Deserialize(serializer.Serialize(value)));
        }

        private void RoundTripStream<T>(T value) {
            var serializer = SerializerRepository.GetStreamSerializer<T>();
            var ms = new MemoryStream();
            serializer.Serialize(ms, value);
            ms.Position = 0;
            Assert.AreEqual(value, serializer.Deserialize(ms));
        }

        [Serializable]
        public class Serializable {
            public int Id;
            public string Name;

            public override bool Equals(object obj) {
                var right = obj as Serializable;
                if(right == null) {
                    return false;
                }
                return Id == right.Id && Name == right.Name;
            }
        }
    }
}
