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
        private static readonly Dictionary<Type, object> _byteArraySerializers = new Dictionary<Type, object>();
        private static readonly Dictionary<Type, object> _streamSerializers = new Dictionary<Type, object>();

        //--- Class Constructor ---
        static SerializerRepository() {
            Initialize();
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
        public static void RegisterByteArraySerializer<T>(IByteArraySerializer<T> serializer) {
            if(serializer == null) {
                throw new ArgumentNullException("serializer");
            }
            lock(_byteArraySerializers) {
                _byteArraySerializers[typeof(T)] = serializer;
            }
        }

        public static void RegisterStreamSerializer<T>(IStreamSerializer<T> serializer) {
            if(serializer == null) {
                throw new ArgumentNullException("serializer");
            }
            lock(_streamSerializers) {
                _streamSerializers[typeof(T)] = serializer;
            }
        }

        public static IByteArraySerializer<T> GetByteArraySerializer<T>() {
            return GetByteArraySerializer<T>(true);
        }

        private static IByteArraySerializer<T> GetByteArraySerializer<T>(bool throwOnUnkown) {
            var t = typeof(T);
            object serializer;
            lock(_byteArraySerializers) {
                if(!_byteArraySerializers.TryGetValue(t, out serializer)) {
                    serializer = _serializerGenerator.GenerateByteArraySerializer<T>();
                    if(serializer == null) {
                        if(throwOnUnkown) {
                            throw new KeyNotFoundException(string.Format("No key serializer for type {0} found.", t));
                        }
                        return null;
                    }
                    _byteArraySerializers[t] = serializer;
                }
            }
            return (IByteArraySerializer<T>)serializer;
        }

        public static IStreamSerializer<T> GetStreamSerializer<T>() {
            return GetStreamSerializer<T>(true);
        }

        private static IStreamSerializer<T> GetStreamSerializer<T>(bool throwOnUnkown) {
            var t = typeof(T);
            object serializer;
            lock(_streamSerializers) {
                if(!_streamSerializers.TryGetValue(t, out serializer)) {
                    serializer = _serializerGenerator.GenerateStreamSerializer<T>();
                    if(serializer == null) {
                        if(throwOnUnkown) {
                            throw new KeyNotFoundException(string.Format("No value serializer for type {0} found.", t));
                        }
                        return null;
                    }
                    _streamSerializers[t] = serializer;
                }
            }
            return (IStreamSerializer<T>)serializer;
        }

        public static bool HasByteArraySerializer<T>() {
            return GetByteArraySerializer<T>(false) != null;
        }

        public static bool HasStreamSerializer<T>() {
            return GetStreamSerializer<T>(false) != null;
        }

        public static void Reset() {
            lock(_byteArraySerializers) {
                lock(_streamSerializers) {
                    _byteArraySerializers.Clear();
                    _streamSerializers.Clear();
                    Initialize();
                }
            }
        }

        private static void Initialize() {
            SerializerGenerator = new BinaryFormatterGenerator();

            // short default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<short>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToInt16(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<short>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt16(stream.ReadBytes(), 0)
            });

            // ushort default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<ushort>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToUInt16(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<ushort>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt16(stream.ReadBytes(), 0)
            });

            // int default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<int>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToInt32(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<int>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt32(stream.ReadBytes(), 0)
            });

            // uint default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<uint>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToUInt32(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<uint>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt32(stream.ReadBytes(), 0)
            });

            // long default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<long>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToInt64(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<long>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToInt64(stream.ReadBytes(), 0)
            });

            // ulong default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<ulong>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToUInt64(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<ulong>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToUInt64(stream.ReadBytes(), 0)
            });

            // float default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<float>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToSingle(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<float>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToSingle(stream.ReadBytes(), 0)
            });

            // double default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<double>() {
                Serializer = value => BitConverter.GetBytes(value),
                Deserializer = bytes => BitConverter.ToDouble(bytes, 0)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<double>() {
                Serializer = (stream, value) => stream.Write(BitConverter.GetBytes(value)),
                Deserializer = stream => BitConverter.ToDouble(stream.ReadBytes(), 0)
            });

            // string default serializers
            RegisterByteArraySerializer(new ByteArraySerializationLambdaWrapper<string>() {
                Serializer = value => Encoding.UTF8.GetBytes(value),
                Deserializer = bytes => Encoding.UTF8.GetString(bytes)
            });
            RegisterStreamSerializer(new StreamSerializationLambdaWrapper<string>() {
                Serializer = (stream, value) => stream.Write(Encoding.UTF8.GetBytes(value)),
                Deserializer = stream => Encoding.UTF8.GetString(stream.ReadBytes())
            });
        }
    }
}
