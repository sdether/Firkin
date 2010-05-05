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
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using Droog.Firkin.Test.Perf.Stackoverflow;
using Droog.Firkin.Util;
using log4net;
using NUnit.Framework;
using ProtoBuf;

namespace Droog.Firkin.Test.Perf {

    // Note: This test assumes that the 042010 StackOverflow data dump lives at C:\data\042010 SO
    [TestFixture]
    public class TStackoverflow {

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        [Test]
        public void Read_Posts_from_Xml() {
            Console.WriteLine("All posts: {0}", Diagnostics.Time(() => {
                var i = 0;
                foreach(var user in ReadEntitiesFromXml<User>(@"C:\data\042010 SO\posts.xml")) {
                    i++;
                    //Console.WriteLine(user.DisplayName);
                }
                Console.WriteLine("Total: {0}", i);
            }));
        }

        [Test]
        public void Read_Users_from_Xml() {
            Console.WriteLine("All users: {0}", Diagnostics.Time(() => {
                var i = 0;
                foreach(var user in ReadEntitiesFromXml<User>(@"C:\data\042010 SO\users.xml")) {
                    i++;
                    //Console.WriteLine(user.DisplayName);
                }
                Console.WriteLine("Total: {0}", i);
            }));
        }

        [Test]
        public void Read_write_users_with_Firkin() {
            Dictionary<int, Stream> users = null;
            var elapsed = Diagnostics.Time(() => {
                users = (from user in ReadEntitiesFromXml<User>(@"C:\data\042010 SO\users.xml")
                         select new {
                             user.Id,
                             Stream = GetUserStream(user)
                         })
                    .ToDictionary(x => x.Id, y => y.Stream);
            });
            _log.DebugFormat("Read {0} users from xml: {1} ({2:0}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new FirkinHash<int>(path);
            try {
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                _log.DebugFormat("Wrote {0} users to firkin: {1} ({2:0}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
                var comp = new List<Stream[]>();
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users.OrderBy(x => x.Value.Length)) {
                        var stream = hash.Get(user.Key);
                        comp.Add(new[] { stream, user.Value });
                    }
                });
                _log.DebugFormat("Queried {0} users from firkin: {1} ({2:0}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
                foreach(var pair in comp) {
                    pair[0].Position = 0;
                    pair[1].Position = 0;
                    Assert.AreEqual(pair[0].ReadBytes(pair[0].Length), pair[1].ReadBytes(pair[1].Length));
                }
            } finally {
                hash.Dispose();
                Directory.Delete(path, true);
            }

        }

        [Test]
        public void Read_write_users_with_hash_reload() {
            Dictionary<int, Stream> users = null;
            var elapsed = Diagnostics.Time(() => {
                users = (from user in ReadEntitiesFromXml<User>(@"C:\data\042010 SO\users.xml")
                         select new {
                             user.Id,
                             Stream = GetUserStream(user)
                         })
                    .ToDictionary(x => x.Id, y => y.Stream);
            });
            _log.DebugFormat("Read {0} users from xml: {1} ({2:0.0000}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new FirkinHash<int>(path);
            try {
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                _log.DebugFormat("Wrote {0} users to firkin: {1} ({2:0.0000}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
                hash.Dispose();
                _log.DebugFormat("re-loading hash");
                hash = new FirkinHash<int>(path);
                var comp = new List<Stream[]>();
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users.OrderBy(x => x.Value.Length)) {
                        var stream = hash.Get(user.Key);
                        comp.Add(new[] { stream, user.Value });
                    }
                });
                _log.DebugFormat("Queried {0} users from firkin: {1} ({2:0.0000}users/second)", users.Count, elapsed, users.Count / elapsed.TotalSeconds);
                foreach(var pair in comp) {
                    pair[0].Position = 0;
                    pair[1].Position = 0;
                    Assert.AreEqual(pair[0].ReadBytes(pair[0].Length), pair[1].ReadBytes(pair[1].Length));
                }
            } finally {
                hash.Dispose();
                Directory.Delete(path, true);
            }

        }

        private Stream GetUserStream(User user) {
            var stream = new MemoryStream();
            Serializer.Serialize(stream, user);
            stream.Position = 0;
            return stream;
        }

        public IEnumerable<T> ReadEntitiesFromXml<T>(string filename) {
            var serializer = new XmlSerializer(typeof(T));
            var stream = new FileStream(filename, FileMode.Open);
            var reader = new XmlTextReader(stream);
            while(reader.Read()) {
                if(reader.NodeType != XmlNodeType.Element) {
                    continue;
                }
                if(reader.Name != "row") {
                    continue;
                }
                yield return (T)serializer.Deserialize(reader);
            }
        }
    }
}
