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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Droog.Firkin.Serialization;

namespace Droog.Firkin {
    public class FirkinDictionary<TKey,TValue> : IDictionary<TKey,TValue> {

        //--- Fields ---
        private readonly IFirkinHash<TKey> _hash;
        private readonly IStreamSerializer<TValue> _valueSerializer;

        //--- Constructors ---
        public FirkinDictionary(string storageDirectory, long maxFileSize, IKeySerializer<TKey> keySerializer, IStreamSerializer<TValue> valueSerializer) {
            _valueSerializer = valueSerializer;
            _hash = new FirkinHash<TKey>(storageDirectory, maxFileSize, keySerializer);
        }

        public FirkinDictionary(string storageDirectory) {
            _hash = new FirkinHash<TKey>(storageDirectory);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
            foreach(var pair in _hash) {
                yield return new KeyValuePair<TKey, TValue>(pair.Key,_valueSerializer.Deserialize(pair.Value));
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<TKey, TValue> item) {
            var stream = GetStream(item);
            _hash.Put(item.Key,stream,stream.Length);
        }

        public void Clear() {
            _hash.Truncate();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) {
            throw new NotImplementedException();
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) {
            throw new NotImplementedException();
        }

        public bool Remove(KeyValuePair<TKey, TValue> item) {
            throw new NotImplementedException();
        }

        public int Count {
            get { throw new NotImplementedException(); }
        }

        public bool IsReadOnly {
            get { throw new NotImplementedException(); }
        }

        public bool ContainsKey(TKey key) {
            throw new NotImplementedException();
        }

        public void Add(TKey key, TValue value) {
            throw new NotImplementedException();
        }

        public bool Remove(TKey key) {
            throw new NotImplementedException();
        }

        public bool TryGetValue(TKey key, out TValue value) {
            throw new NotImplementedException();
        }

        public TValue this[TKey key] {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public ICollection<TKey> Keys {
            get { throw new NotImplementedException(); }
        }

        public ICollection<TValue> Values {
            get { throw new NotImplementedException(); }
        }

        private MemoryStream GetStream(KeyValuePair<TKey, TValue> item) {
            var stream = new MemoryStream();
            _valueSerializer.Serialize(stream,item.Value);
            stream.Position = 0;
            return stream;
        }
    }
}
