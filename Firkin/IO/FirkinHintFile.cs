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
using Droog.Firkin.Data;
using Droog.Firkin.Util;
using log4net;

namespace Droog.Firkin.IO {
    public class FirkinHintFile : IFirkinHintFile {
        private readonly string _filename;
        private const int HEADER_SIZE = 4 + 4 + 4 + 4;
        private const int SERIAL_OFFSET = 0;
        private const int KEY_SIZE_OFFSET = SERIAL_OFFSET + 4;
        private const int VALUE_SIZE_OFFSET = KEY_SIZE_OFFSET + 4;
        private const int VALUE_POSITION_OFFSET = VALUE_SIZE_OFFSET + 4;

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly Stream _stream;

        public FirkinHintFile(string filename) {
            _filename = filename;
            _stream = File.Open(filename, FileMode.OpenOrCreate);
            _log.DebugFormat("opened hint file '{0}' ", Path.GetFileName(_filename));
        }

        public void WriteHint(KeyValueRecord data, uint valuePosition) {
            lock(_stream) {
                _stream.Write(BitConverter.GetBytes(data.Serial));
                _stream.Write(BitConverter.GetBytes((uint)data.Key.LongLength));
                _stream.Write(BitConverter.GetBytes(data.ValueSize));
                _stream.Write(BitConverter.GetBytes(valuePosition));
                _stream.Write(data.Key);
            }
        }

        public void Dispose() {
            _log.DebugFormat("disposing hint file '{0}' ", Path.GetFileName(_filename));
            _stream.Close();
            _stream.Dispose();
        }

        public IEnumerator<HintRecord> GetEnumerator() {
            lock(_stream) {
                _stream.Position = 0;
                var keyCounter = 0;
                while(true) {
                    var header = _stream.ReadBytes(HEADER_SIZE);
                    if(header.Length == 0) {

                        // end of file
                        yield break;
                    }
                    keyCounter++;
                    var serial = BitConverter.ToUInt32(header, SERIAL_OFFSET);
                    var keySize = BitConverter.ToUInt32(header, KEY_SIZE_OFFSET);
                    if(keySize > FirkinHash<object>.MaxKeySize) {
                        var error = string.Format("Hint Enumerator: key {0} in file '{1}' had key of size {2}", keyCounter, _filename, keySize);
                        throw new CorruptKeyException(error);
                    }
                    var valueSize = BitConverter.ToUInt32(header, VALUE_SIZE_OFFSET);
                    var valuePosition = BitConverter.ToUInt32(header, VALUE_POSITION_OFFSET);
                    var key = _stream.ReadBytes(keySize);
                    yield return new HintRecord() {
                        Key = key,
                        KeySize = keySize,
                        Serial = serial,
                        ValuePosition = valuePosition,
                        ValueSize = valueSize
                    };
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }
    }
}