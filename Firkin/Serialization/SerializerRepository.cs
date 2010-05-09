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
using System.Threading;
using Droog.Firkin.Util;

namespace Droog.Firkin.Serialization {
    public static class SerializerRepository {

        //--- Class Fields ---
        private static ISerializerGenerator _serializerGenerator;
        private static readonly Dictionary<Type, object> _keySerializers = new Dictionary<Type, object>();
        private static readonly Dictionary<Type, object> _valueSerializers = new Dictionary<Type, object>();

        //--- Class Constructor ---
        static SerializerRepository() {
            SerializerGenerator = new BinaryFormatterGenerator();

            // short default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<short>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToInt16(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<short>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt16(stream.ReadBytes(), 0)
            });

            // ushort default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<ushort>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToUInt16(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<ushort>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt16(stream.ReadBytes(), 0)
            });

            // int default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<int>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToInt32(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<int>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt32(stream.ReadBytes(), 0)
            });

            // uint default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<uint>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToUInt32(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<uint>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt32(stream.ReadBytes(), 0)
            });

            // long default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<long>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToInt64(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<long>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt64(stream.ReadBytes(), 0)
            });

            // ulong default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<ulong>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToUInt64(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<ulong>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt64(stream.ReadBytes(), 0)
            });

            // float default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<float>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToSingle(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<float>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToSingle(stream.ReadBytes(), 0)
            });

            // double default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<double>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToDouble(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<double>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToDouble(stream.ReadBytes(), 0)
            });

            // double default serializers
            RegisterKeySerializer(new KeySerializationLambdaWrapper<double>() {
                Serializer = key => BitConverter.GetBytes(key),
                Deserializer = bytes => BitConverter.ToDouble(bytes, 0)
            });
            RegisterValueSerializer(new ValueSerializationLambdaWrapper<double>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToDouble(stream.ReadBytes(), 0)
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

        //--- Class Properties ---
        public static ISerializerGenerator SerializerGenerator {
            get { return _serializerGenerator; }
            set {
                if(value == null) {
                    throw new ArgumentNullException();
                }
                _serializerGenerator = value;
            }
        }

        //--- Class Methods ---
        public static void RegisterKeySerializer<TKey>(IByteArraySerializer<TKey> serializer) {
            if(serializer == null) {
                throw new ArgumentNullException("serializer");
            }
            lock(_keySerializers) {
                _keySerializers[typeof(TKey)] = serializer;
            }
        }

        public static void RegisterValueSerializer<TValue>(IStreamSerializer<TValue> serializer) {
            if(serializer == null) {
                throw new ArgumentNullException("serializer");
            }
            lock(_valueSerializers) {
                _valueSerializers[typeof(TValue)] = serializer;
            }
        }

        public static IByteArraySerializer<TKey> GetKeySerializer<TKey>() {
            return GetKeySerializer<TKey>(true);
        }

        private static IByteArraySerializer<TKey> GetKeySerializer<TKey>(bool throwOnUnkown) {
            var t = typeof(TKey);
            object serializer;
            lock(_keySerializers) {
                if(!_keySerializers.TryGetValue(t, out serializer)) {
                    serializer = _serializerGenerator.GenerateByteArraySerializer<TKey>();
                    if(serializer == null) {
                        if(throwOnUnkown) {
                            throw new KeyNotFoundException(string.Format("No key serializer for type {0} found.", t));
                        }
                        return null;
                    }
                    _keySerializers[t] = serializer;
                }
            }
            return (IByteArraySerializer<TKey>)serializer;
        }

        public static IStreamSerializer<TValue> GetValueSerializer<TValue>() {
            return GetValueSerializer<TValue>(true);
        }

        private static IStreamSerializer<TValue> GetValueSerializer<TValue>(bool throwOnUnkown) {
            var t = typeof(TValue);
            object serializer;
            lock(_valueSerializers) {
                if(!_valueSerializers.TryGetValue(t, out serializer)) {
                    serializer = _serializerGenerator.GenerateByteArraySerializer<TValue>();
                    if(serializer == null) {
                        if(throwOnUnkown) {
                            throw new KeyNotFoundException(string.Format("No value serializer for type {0} found.", t));
                        }
                        return null;
                    }
                    _valueSerializers[t] = serializer;
                }
            }
            return (IStreamSerializer<TValue>)serializer;
        }

        public static bool HasKeySerializer<TKey>() {
            return GetKeySerializer<TKey>() == null;
        }

        public static bool HasValueSerializer<TValue>() {
            return GetValueSerializer<TValue>() == null;
        }
    }
}
