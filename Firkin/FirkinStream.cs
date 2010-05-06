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
using Droog.Firkin.IO;
using log4net;

namespace Droog.Firkin {
    public class FirkinStream : Stream {

        //--- Constants ---
        public const int BUFFER_SIZE = 16 * 1024;

        //--- Class Fields ---
        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        //--- Fields ---
        private readonly StreamSyncRoot _streamSyncRoot;
        private readonly Stream _stream;
        private readonly long _offset;
        private readonly long _length;
        private long _position;
        private byte[] _current;
        private int _currentPosition;
        private List<byte[]> _chunks;
        private int _chunkIndex;
        private bool _isMemorized;
        private bool _isDisposed;

        //--- Constructors ---
        public FirkinStream(StreamSyncRoot streamSyncRoot, Stream stream, long offset, long length) {
            _streamSyncRoot = streamSyncRoot;
            _stream = stream;
            _offset = offset;
            _length = length;
            _position = _offset;
            if(_length <= BUFFER_SIZE) {
                Memorize();
            }
        }

        //--- Properties ---
        public bool IsDisposed { get { return _streamSyncRoot.IsDisposed || _isDisposed; } }
        public bool IsMemorized { get { return _isMemorized; } }
        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return false; } }
        public override long Length { get { return _length; } }

        public override long Position {
            get { return _position - _offset; }
            set { throw new InvalidOperationException(); }
        }

        //--- Methods ---
        public void Memorize() {
            if(_isMemorized) {
                return;
            }
            if(_offset != _position) {
                throw new InvalidOperationException("Cannot memorize a FirkinStream after reading has already begun");
            }
            _chunks = new List<byte[]>();
            lock(_streamSyncRoot) {
                CheckObjectDisposed();
                _stream.Position = _offset;
                while(_position < _offset + Length) {
                    var buffer = new byte[Math.Min(BUFFER_SIZE, _offset + _length - _position)];
                    var read = _stream.Read(buffer, 0, buffer.Length);
                    if(_current == null) {
                        _current = buffer;
                    }
                    _chunks.Add(buffer);
                    _position += read;
                }
            }
            _isMemorized = true;
        }

        public override int Read(byte[] buffer, int offset, int count) {
            var read = 0;
            while(count > 0) {
                if(_current == null || _currentPosition == _current.Length) {
                    if(_isMemorized) {
                        _chunkIndex++;
                        if(_chunkIndex >= _chunks.Count) {
                            return read;
                        }
                        _currentPosition = 0;
                        _current = _chunks[_chunkIndex];
                    } else {
                        lock(_streamSyncRoot) {
                            _position += _currentPosition;
                            if(_position >= _offset + _length) {
                                return read;
                            }
                            _stream.Position = _position;
                            _current = new byte[BUFFER_SIZE];
                            _stream.Read(_current, 0, Math.Min(BUFFER_SIZE,(int)(_offset + _length - _position)));
                            _currentPosition = 0;
                        }
                    }
                }
                var copyCount = Math.Min(count, _current.Length - _currentPosition);
                Array.Copy(_current, _currentPosition, buffer, offset, copyCount);
                _currentPosition += copyCount;
                read += copyCount;
                offset += copyCount;
                count -= copyCount;
            }
            return read;
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) {
            throw new InvalidOperationException();
        }

        public override void SetLength(long value) {
            throw new InvalidOperationException();
        }

        public override void Write(byte[] buffer, int offset, int count) {
            throw new InvalidOperationException();
        }

        protected override void Dispose(bool disposing) {
            base.Dispose(disposing);
            _isDisposed = true;
        }

        private void CheckObjectDisposed() {
            if(IsDisposed) {
                throw new ObjectDisposedException(ToString());
            }
        }
    }
}