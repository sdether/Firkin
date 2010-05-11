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
using Droog.Firkin.IO;

namespace Droog.Firkin.Data {
    public struct KeyInfo {

        public ushort FileId;
        public uint Serial;
        public uint ValueSize;
        public uint ValuePosition;

        public KeyInfo(ushort fileId, HintRecord hint) {
            FileId = fileId;
            Serial = hint.Serial;
            ValueSize = hint.ValueSize;
            ValuePosition = hint.ValuePosition;
        }

        public uint GetSize() {
            return FirkinFile.HEADER_SIZE + ValueSize;
        }
    }
}