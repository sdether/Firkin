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
using System.IO;

namespace Droog.Firkin.IO {
    public class StreamView : Stream {
        private readonly Stream _stream;
        private readonly int _offset;
        private readonly int _length;
        private readonly int _position;

        public StreamView(Stream stream, int offset, int length) {
            _stream = stream;
            _offset = offset;
            _length = length;
            _position = _offset;
        }

        public override int Read(byte[] buffer, int offset, int count) {
            if(offset > _length - _position) {
                return 0;
            }
            return _stream.Read(buffer, _offset + _position + offset, Math.Min(count, _length - _position));
        }

        public override long Position {
            get { return _position; }
            set { throw new NotImplementedException(); }
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return false; } }
        public override long Length { get { return _length; } }

        public override void Flush() {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin) {
            throw new NotImplementedException();
        }

        public override void SetLength(long value) {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count) {
            throw new NotImplementedException();
        }
    }
}