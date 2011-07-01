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
    public class FirkinFile : IFirkinArchiveFile, IFirkinActiveFile {

        private const int HASH_SIZE = 16;
        private const int HEADER_SIZE = HASH_SIZE + 4 + 4 + 4;
        private const int SERIAL_OFFSET = HASH_SIZE;
        private const int KEY_SIZE_OFFSET = SERIAL_OFFSET + 4;
        private const int VALUE_SIZE_OFFSET = KEY_SIZE_OFFSET + 4;

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private readonly ushort _fileId;
        private string _filename;
        private readonly bool _write;
        private readonly StreamSyncRoot _streamSyncRoot = new StreamSyncRoot();
        private Stream _stream;
        private uint _serial;

        public static IFirkinArchiveFile OpenArchive(string filename, ushort fileId) {
            return new FirkinFile(filename, fileId, false, 0);
        }

        public static IFirkinArchiveFile OpenArchiveFromActive(IFirkinActiveFile file) {
            file.Dispose();
            return new FirkinFile(file.Filename, file.FileId, false, 0);
        }

        public static IFirkinActiveFile OpenActiveFromArchive(IFirkinArchiveFile file, uint serial) {
            file.Dispose();
            return new FirkinFile(file.Filename, file.FileId, true, serial);
        }

        public static IFirkinActiveFile CreateActive(string filename, ushort fileId) {
            return new FirkinFile(filename, fileId, true, 0);
        }

        private FirkinFile(string filename, ushort fileId, bool write, uint serial) {
            _filename = filename;
            _fileId = fileId;
            _write = write;
            _serial = serial;
            _stream = write ? File.Open(filename, FileMode.OpenOrCreate) : File.OpenRead(filename);
            _log.DebugFormat("opened {0} file '{1}' as id {2}", _write ? "read/write" : "read-only", Path.GetFileName(_filename), _fileId);
        }

        public string Filename { get { return _filename; } }
        public ushort FileId { get { return _fileId; } }
        public long Size { get { return _stream.Length; } }
        public uint Serial { get { return _serial; } }

        public KeyInfo Write(KeyValuePair data) {
            if(!_write) {
                throw new InvalidOperationException("cannot write to readonly file");
            }
            using(var dataStream = new MemoryStream()) {
                byte[] hashBytes;
                dataStream.Write(data.Key);
                if(data.ValueSize > 0) {
                    data.Value.CopyTo(dataStream, data.ValueSize);
                    dataStream.Position = 0;
                    hashBytes = dataStream.ComputeHash();
                } else {
                    hashBytes = new byte[HASH_SIZE];
                }
                dataStream.Position = 0;
                uint serial = 0;
                uint valuePosition;
                lock(_streamSyncRoot) {
                    serial = ++_serial;
                    _stream.Seek(0, SeekOrigin.End);
                    _stream.Write(hashBytes);
                    _stream.Write(BitConverter.GetBytes(serial));
                    _stream.Write(BitConverter.GetBytes((uint)data.Key.LongLength));
                    _stream.Write(BitConverter.GetBytes(data.ValueSize));
                    dataStream.Position = 0;
                    dataStream.CopyTo(_stream, dataStream.Length);
                    valuePosition = (uint)_stream.Position - data.ValueSize;
                }
                return new KeyInfo() {
                    FileId = FileId,
                    ValueSize = data.ValueSize,
                    ValuePosition = valuePosition,
                    Serial = serial
                };
            }
        }

        public uint Write(KeyValueRecord data) {
            if(!_write) {
                throw new InvalidOperationException("cannot write to readonly file");
            }
            lock(_streamSyncRoot) {
                _stream.Seek(0, SeekOrigin.End);
                _stream.Write(data.Hash);
                _stream.Write(BitConverter.GetBytes(data.Serial));
                _stream.Write(BitConverter.GetBytes((uint)data.Key.LongLength));
                _stream.Write(BitConverter.GetBytes(data.ValueSize));
                _stream.Write(data.Key);
                data.Value.CopyTo(_stream, data.ValueSize);
                return (uint)_stream.Position - data.ValueSize;
            }
        }

        public FirkinStream ReadValue(KeyInfo keyInfo) {
            return new FirkinStream(_streamSyncRoot, _stream, (long)keyInfo.ValuePosition, (long)keyInfo.ValueSize);
        }

        public IEnumerable<KeyValueRecord> GetRecords() {
            lock(_streamSyncRoot) {
                CheckObjectDisposed();
                _stream.Position = 0;
                while(true) {

                    // TODO: combine head logic with GetKeys()
                    var header = _stream.ReadBytes(HEADER_SIZE);
                    if(header.Length == 0) {

                        // end of file
                        yield break;
                    }
                    var keySize = BitConverter.ToUInt32(header, KEY_SIZE_OFFSET);
                    var valueSize = BitConverter.ToUInt32(header, VALUE_SIZE_OFFSET);
                    var key = _stream.ReadBytes(keySize);
                    var value = new MemoryStream();
                    _stream.CopyTo(value, valueSize);
                    var recordHash = header.Select(0, HASH_SIZE);
                    using(var hashable = new MemoryStream()) {
                        value.Position = 0;
                        hashable.Write(key);
                        value.CopyTo(hashable, valueSize);
                        hashable.Position = 0;
                        var computedHash = hashable.ComputeHash();
                        if(recordHash.Compare(computedHash) != 0) {

                            // currently just skipping corrupt records
                            continue;
                        }
                    }
                    value.Position = 0;
                    yield return new KeyValueRecord() {
                        Hash = recordHash,
                        Serial = BitConverter.ToUInt32(header, SERIAL_OFFSET),
                        ValueSize = valueSize,
                        Key = key,
                        Value = value
                    };
                }
            }
        }

        public void Rename(string newFilename) {
            lock(_streamSyncRoot) {
                CheckObjectDisposed();
                _stream.Close();
                _stream.Dispose();
                File.Move(_filename, newFilename);
                _filename = newFilename;
                _stream = _write ? File.Open(_filename, FileMode.OpenOrCreate) : File.OpenRead(_filename);
            }
        }

        public void Flush() {
            lock(_streamSyncRoot) {
                CheckObjectDisposed();
                _stream.Flush();
            }
        }

        public IEnumerable<KeyValuePair<byte[], KeyInfo>> GetKeys() {
            lock(_streamSyncRoot) {
                CheckObjectDisposed();
                _stream.Position = 0;
                while(true) {
                    var recordPosition = _stream.Position;
                    var header = _stream.ReadBytes(HEADER_SIZE);
                    if(header.Length == 0) {

                        // end of file
                        yield break;
                    }
                    var keySize = BitConverter.ToUInt32(header, KEY_SIZE_OFFSET);
                    var valueSize = BitConverter.ToUInt32(header, VALUE_SIZE_OFFSET);
                    var key = _stream.ReadBytes(keySize);
                    _stream.Seek(valueSize, SeekOrigin.Current);
                    yield return new KeyValuePair<byte[], KeyInfo>(
                        key,
                        new KeyInfo() {
                            FileId = FileId,
                            Serial = BitConverter.ToUInt32(header, SERIAL_OFFSET),
                            ValuePosition = (uint)(recordPosition + HEADER_SIZE + keySize),
                            ValueSize = valueSize
                        });
                }
            }
        }

        public void Dispose() {
            if(!_streamSyncRoot.IsDisposed) {
                _log.DebugFormat("disposing file '{0}'", Path.GetFileName(_filename));
                _stream.Close();
                _stream.Dispose();
                _streamSyncRoot.IsDisposed = true;
            }
        }

        public IEnumerator<KeyValuePair<byte[], KeyInfo>> GetEnumerator() {
            CheckObjectDisposed();
            return GetKeys().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        private void CheckObjectDisposed() {
            if(_streamSyncRoot.IsDisposed) {
                throw new ObjectDisposedException(ToString());
            }
        }
    }
}