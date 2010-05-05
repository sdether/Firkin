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
using Droog.Firkin.Data;
using NUnit.Framework;

namespace Droog.Firkin.Test {

    [TestFixture]
    public class TFirkinFile {

        [Test]
        public void Can_rename_file() {
            var f1 = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var f2 = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var file = FirkinFile.CreateActive(f1, 1);
            var data = new MemoryStream();
            data.WriteByte(2);
            data.Position = 0;
            var keyInfo = file.Write(new KeyValuePair() { Key = new byte[] { 1 }, Value = data, ValueSize = (uint)data.Length });
            try {
                file.Rename(f2);
                Assert.IsFalse(File.Exists(f1));
                Assert.IsTrue(File.Exists(f2));
                var stream = file.ReadValue(keyInfo);
                Assert.AreEqual(1, stream.Length);
                Assert.AreEqual(2, stream.ReadByte());
                file.Dispose();
            } finally {
                File.Delete(f1);
                File.Delete(f2);
            }
        }
    }
}
