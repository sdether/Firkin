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
using System.Text;

namespace Droog.Firkin.Test {
    public static class TestUtil {
        public static string GetRandomString(Random r) {
            var builder = new StringBuilder();
            var n = r.Next(50) + 30;
            for(var i = 0; i < n; i++) {
                builder.Append(Guid.NewGuid());
            }
            return builder.ToString();
        }

        public static byte[] GetRandomBytes(Random r) {
            var bytes = new byte[r.Next(50) + 50];
            for(var i = 0; i < bytes.Length; i++) {
                bytes[i] = (byte)(r.Next(30) + 10);
            }
            return bytes;
        }

        public static Stream ToStream(this byte[] bytes) {
            var stream = new MemoryStream();
            stream.Write(bytes, 0, bytes.Length);
            stream.Position = 0;
            return stream;
        }

        public static Stream ToStream(this int value) {
            var stream = new MemoryStream();
            var bytes = BitConverter.GetBytes(value);
            stream.Write(bytes, 0, bytes.Length);
            stream.Position = 0;
            return stream;
        }

        public static Stream ToStream(this string value) {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(value);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }

        public static T To<T>(this Stream stream) {
            if(typeof(T) == typeof(int)) {
                var bytes = new Byte[4];
                stream.Read(bytes, 0, 4);
                return (T)(object)BitConverter.ToInt32(bytes, 0);
            }
            var reader = new StreamReader(stream);
            return (T)(object)reader.ReadToEnd();
        }
    }
}
