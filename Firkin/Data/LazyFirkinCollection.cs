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
using System.Linq;

namespace Droog.Firkin.Data {
    public class LazyFirkinCollection<TKey, TValue> : ICollection<TValue> {
        private readonly ICollection<TKey> _keys;
        private readonly Func<TKey, FirkinStream> _getStream;
        private readonly TryDeserializeDelegate<TKey, TValue> _getValue;

        public LazyFirkinCollection(ICollection<TKey> keys, Func<TKey, FirkinStream> getStream, TryDeserializeDelegate<TKey, TValue> getValue) {
            _keys = keys;
            _getStream = getStream;
            _getValue = getValue;
        }

        public IEnumerator<TValue> GetEnumerator() {
            foreach(var key in _keys) {
                var s = _getStream(key);
                if(s == null) {
                    continue;
                }
                TValue value;
                if(_getValue(key, s, out value)) {
                    yield return value;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        public void Add(TValue item) { }

        public void Clear() { }

        public bool Contains(TValue item) {
            return false;
        }

        public void CopyTo(TValue[] array, int arrayIndex) {
            var source = this.Take(array.Length - arrayIndex).ToArray();
            Array.Copy(source, 0, array, arrayIndex, source.Length);
        }

        public bool Remove(TValue item) {
            return false;
        }

        public int Count { get { return _keys.Count; } }

        public bool IsReadOnly { get { return true; } }
    }

    public delegate bool TryDeserializeDelegate<TKey, TValue>(TKey key, FirkinStream stream, out TValue value);
}