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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Droog.Firkin.Serialization {
    public class BinaryFormatterGenerator : ISerializerGenerator {
        private static BinaryFormatter _serializer = new BinaryFormatter();
        public IByteArraySerializer<TKey> GenerateByteArraySerializer<TKey>() {
            var t = typeof(TKey);
            if(!t.IsSerializable) {
                return null;
            }
            return new ByteArraySerializationLambdaWrapper<TKey>() {
                Serializer = key => {
                    using(var ms = new MemoryStream()) {
                        _serializer.Serialize(ms, key);
                        return ms.ToArray();
                    }
                },
                Deserializer = bytes => {
                    using(var ms = new MemoryStream(bytes)) {
                        return (TKey)_serializer.Deserialize(ms);
                    }
                }
            };
        }

        public IStreamSerializer<TValue> GenerateStreamSerializer<TValue>() {
            var t = typeof(TValue);
            if(!t.IsSerializable) {
                return null;
            }
            return new StreamSerializationLambdaWrapper<TValue>() {
                Serializer = (stream, value) => _serializer.Serialize(stream,value),
                Deserializer = stream => (TValue)_serializer.Deserialize(stream)
            };
        }
    }
}