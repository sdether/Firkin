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
    public class TSerializerRepository {

        [SetUp]
        public void Setup() {
            SerializerRepository.Reset();    
        }

        [Test]
        public void Can_get_int_bytearray_serializer() {
            Assert.IsNotNull(SerializerRepository.GetByteArraySerializer<int>());
        }

        [Test]
        public void Can_get_int_stream_serializer() {
            Assert.IsNotNull(SerializerRepository.GetStreamSerializer<int>());
        }

        [Test]
        public void HasByteArraySerializer_returns_false_for_unknown_type() {
            Assert.IsFalse(SerializerRepository.HasByteArraySerializer<TypeToSerialize>());
        }

        [Test]
        public void HasStreamSerializer_returns_false_for_unknown_type() {
            Assert.IsFalse(SerializerRepository.HasStreamSerializer<TypeToSerialize>());
        }

        [Test]
        public void HasByteArraySerializer_returns_true_for_Serializable_type_with_default_generator() {
            Assert.IsTrue(SerializerRepository.HasByteArraySerializer<Serializable>());
        }

        [Test]
        public void HasStreamSerializer_returns_true_for_Serializable_type_with_default_generator() {
            Assert.IsTrue(SerializerRepository.HasStreamSerializer<Serializable>());
        }

        [Test]
        public void Can_register_custom_bytearray_serializer() {
            var custom = new CustomSerializer();
            SerializerRepository.RegisterByteArraySerializer(custom);
            Assert.AreEqual(custom, SerializerRepository.GetByteArraySerializer<TypeToSerialize>());
        }

        [Test]
        public void Can_register_custom_stream_serializer() {
            var custom = new CustomSerializer();
            SerializerRepository.RegisterStreamSerializer(custom);
            Assert.AreEqual(custom, SerializerRepository.GetStreamSerializer<TypeToSerialize>());
        }

        [Test]
        public void Can_replace_default_string_bytearray_serializer() {
            var custom = new CustomStringSerializer();
            SerializerRepository.RegisterByteArraySerializer(custom);
            Assert.AreEqual(custom, SerializerRepository.GetByteArraySerializer<string>());
        }

        [Test]
        public void Can_replace_default_string_stream_serializer() {
            var custom = new CustomStringSerializer();
            SerializerRepository.RegisterStreamSerializer(custom);
            Assert.AreEqual(custom, SerializerRepository.GetStreamSerializer<string>());
        }

        [Test]
        public void Can_replace_default_serializer_generator() {
            SerializerRepository.SerializerGenerator = new CustomGenerator();
            Assert.IsTrue(SerializerRepository.HasByteArraySerializer<TypeToSerialize>());
            Assert.IsTrue(SerializerRepository.HasStreamSerializer<TypeToSerialize>());
            Assert.AreEqual(typeof(CustomSerializer), SerializerRepository.GetByteArraySerializer<TypeToSerialize>().GetType());
            Assert.AreEqual(typeof(CustomSerializer), SerializerRepository.GetStreamSerializer<TypeToSerialize>().GetType());
        }

        [Serializable]
        public class Serializable {}
        public class TypeToSerialize { }
        public class CustomSerializer : IByteArraySerializer<TypeToSerialize>, IStreamSerializer<TypeToSerialize> {
            public byte[] Serialize(TypeToSerialize key) {
                throw new NotImplementedException();
            }

            public TypeToSerialize Deserialize(byte[] bytes) {
                throw new NotImplementedException();
            }

            public void Serialize(Stream destination, TypeToSerialize value) {
                throw new NotImplementedException();
            }

            public TypeToSerialize Deserialize(Stream source) {
                throw new NotImplementedException();
            }
        }

        public class CustomStringSerializer : IByteArraySerializer<string>, IStreamSerializer<string> {
            public byte[] Serialize(string key) {
                throw new NotImplementedException();
            }

            public string Deserialize(byte[] bytes) {
                throw new NotImplementedException();
            }

            public void Serialize(Stream destination, string value) {
                throw new NotImplementedException();
            }

            public string Deserialize(Stream source) {
                throw new NotImplementedException();
            }
        }

        public class CustomGenerator : ISerializerGenerator {
            public IByteArraySerializer<T> GenerateByteArraySerializer<T>() {
                if(typeof(T) == typeof(TypeToSerialize)) {
                    return (IByteArraySerializer<T>)new CustomSerializer();
                }
                return null;
            }

            public IStreamSerializer<T> GenerateStreamSerializer<T>() {
                if(typeof(T) == typeof(TypeToSerialize)) {
                    return (IStreamSerializer<T>)new CustomSerializer();
                }
                return null;
            }
        }
    }
}
