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
using System.Linq;
using System.Text;
using Droog.Firkin.Util;

namespace Droog.Firkin.Serialization {
    public static class SerializerFactory {

        //--- Class Fields ---
        private static Dictionary<Type, object> _keySerializers = new Dictionary<Type, object>();
        private static Dictionary<Type, object> _valueSerializers = new Dictionary<Type, object>();

        //--- Class Constructor ---
        static SerializerFactory() {

            // int default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<int>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToInt32(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<int>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt32(stream.ReadBytes(), 0)
            });

            // string default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<string>() {
                Serializer = key => Encoding.UTF8.GetBytes(key),
                Deserializer = bytes => Encoding.UTF8.GetString(bytes)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<string>() {
                Serializer = (stream, value) => stream.Write(Encoding.UTF8.GetBytes(value)),
                Deserializer = stream => Encoding.UTF8.GetString(stream.ReadBytes())
            });
        }

        //--- Class Methods ---
        public static void RegisterKeySerializer<TKey>(IByteArraySerializer<TKey> serializer) {
            _keySerializers[typeof(TKey)] = serializer;
        }

        public static void RegisterValueSerializer<TValue>(IStreamSerializer<TValue> serializer) {
            _valueSerializers[typeof(TValue)] = serializer;
        }

        public static IByteArraySerializer<TKey> GetKeySerializer<TKey>() {
            return (IByteArraySerializer<TKey>)_keySerializers[typeof(TKey)];
        }

        public static IStreamSerializer<TValue> GetValueSerializer<TValue>() {
            return (IStreamSerializer<TValue>)_valueSerializers[typeof(TValue)];
        }
    }
}
