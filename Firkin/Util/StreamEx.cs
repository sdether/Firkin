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
using System.Security.Cryptography;

namespace Droog.Firkin.Util {
    public static class StreamEx {

        public const int BUFFER_SIZE = 16 * 1024;

        public static long CopyTo(this Stream source, Stream target, long length) {
            var bufferLength = length >= 0 ? length : long.MaxValue;
            var buffer = new byte[Math.Min(bufferLength, BUFFER_SIZE)];
            long total = 0;
            while(length != 0) {
                var count = (length >= 0) ? Math.Min(length, buffer.LongLength) : buffer.LongLength;
                count = source.Read(buffer, 0, (int)count);
                if(count == 0) {
                    break;
                }
                target.Write(buffer, 0, (int)count);
                total += count;
                length -= count;
            }
            return total;
        }

        public static byte[] ReadBytes(this Stream source) {
             return source.ReadBytes(source.Length);
        }

        public static byte[] ReadBytes(this Stream source, long length) {
            var result = new MemoryStream();
            CopyTo(source, result, length);
            return result.ToArray();
        }

        public static void Write(this Stream stream, byte[] buffer) {
            stream.Write(buffer, 0, buffer.Length);
        }

        public static byte[] ComputeHash(this Stream stream) {
            return MD5.Create().ComputeHash(stream);
        }
    }
}
