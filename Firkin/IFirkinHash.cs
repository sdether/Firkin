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
using System.IO;

namespace Droog.Firkin {
    public interface IFirkinHash<TKey> : IEnumerable<KeyValuePair<TKey, FirkinStream>>, IDisposable {
        void Put(TKey key, Stream stream, uint length);
        void Put(TKey key, Stream stream, long length);
        void Flush();
        FirkinStream Get(TKey key);
        bool Delete(TKey key);
        void Merge();
        void Truncate();
        int Count { get; }
        IEnumerable<TKey> Keys { get; }
        long MaxFileSize { get; }
    }
}