﻿/*
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Droog.Firkin.Data;
using Droog.Firkin.Serialization;
using System.Linq;
using log4net;

namespace Droog.Firkin {
    public class FirkinDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable {

        //--- Class Fields ---
        protected static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        //--- Fields ---
        private readonly IFirkinHash<TKey> _hash;
        private readonly IStreamSerializer<TValue> _valueSerializer;

        //--- Constructors ---
        public FirkinDictionary(string storageDirectory, long maxFileSize, IByteArraySerializer<TKey> keySerializer, IStreamSerializer<TValue> valueSerializer) {
            _valueSerializer = valueSerializer;
            _hash = new FirkinHash<TKey>(storageDirectory, maxFileSize, keySerializer);
        }

        public FirkinDictionary(string storageDirectory) {
            _valueSerializer = SerializerRepository.GetStreamSerializer<TValue>();
            _hash = new FirkinHash<TKey>(storageDirectory);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
            var keys = Keys;
            foreach(var key in keys) {
                var v = _hash.Get(key);
                if(v != null) {
                    TValue value;
                    if(TryDeserialize(key, v, out value)) {
                        yield return new KeyValuePair<TKey, TValue>(key, value);
                    }
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, TValue> item) {
            Add(item.Key, item.Value);
        }

        public void Add(TKey key, TValue value) {

            // Note: This behaves differently from normal dictionaries, as in, it won't throw on collision
            var stream = GetStream(value);
            _hash.Put(key, stream, stream.Length);
        }

        public void Clear() {
            _hash.Truncate();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) {
            var stream = _hash.Get(item.Key);
            return stream != null && item.Value.Equals(_valueSerializer.Deserialize(stream));
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) {
            try {
                foreach(var kvp in this) {
                    array[arrayIndex] = kvp;
                    arrayIndex++;
                }
            } catch(IndexOutOfRangeException e) {
                throw new ArgumentException("Destination array is too small", "array", e);
            }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item) {

            // TODO: race condition.. have put a lock around all _hash accesses to avoid
            return Contains(item) && _hash.Delete(item.Key);
        }

        public int Count {
            get { return _hash.Count; }
        }

        public bool IsReadOnly {
            get { return false; }
        }

        public bool ContainsKey(TKey key) {
            var stream = _hash.Get(key);
            return stream != null;
        }

        public bool Remove(TKey key) {
            return _hash.Delete(key);
        }

        public bool TryGetValue(TKey key, out TValue value) {
            var stream = _hash.Get(key);
            if(stream == null) {
                value = default(TValue);
                return false;
            }
            return TryDeserialize(key, stream, out value);
        }

        private bool TryDeserialize(TKey key, FirkinStream stream, out TValue value) {
            if(stream.Length > _hash.MaxFileSize) {
                var error = string.Format("Stream for key '{0}' was too large, length: {1}. Dictionary is likely corrupted!", key, stream.Length);
                _log.Warn(error);
                throw new CorruptDictionaryException(error);
            }
            value = _valueSerializer.Deserialize(stream);
            return true;
        }

        public TValue this[TKey key] {
            get {
                TValue value;
                if(!TryGetValue(key, out value)) {
                    throw new KeyNotFoundException();
                }
                return value;
            }
            set {
                Add(key, value);
            }
        }

        public ICollection<TKey> Keys {
            get { return _hash.Keys.ToList(); }
        }


        public ICollection<TValue> Values {
            get {
                return new LazyFirkinCollection<TKey, TValue>(Keys, key => _hash.Get(key), TryDeserialize);
            }
        }

        public void Merge() {
            _hash.Merge();
        }

        private MemoryStream GetStream(TValue value) {
            var stream = new MemoryStream();
            _valueSerializer.Serialize(stream, value);
            stream.Position = 0;
            return stream;
        }

        public void Dispose() {
            _hash.Dispose();
        }
    }
}