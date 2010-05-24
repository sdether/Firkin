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
using System.Configuration;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using Droog.Firkin.Test.Perf.Stackoverflow;
using Droog.Firkin.Util;
using Firkin.Reactive;
using log4net;
using NUnit.Framework;
using ProtoBuf;

namespace Droog.Firkin.Test.Perf {

    // Note: This test assumes the 042010 StackOverflow data dump
    [TestFixture]
    public class TStackoverflow {

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        [Test]
        public void Read_write_users_with_Firkin() {
            var users = GetDataSource<User>().ToDictionary(k => k.Id, v => GetEntityStream(v));
            if(!users.Any()) {
                return;
            }
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new FirkinHash<int>(path);
            try {
                var elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                Console.WriteLine("Wrote {0} users to firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
                var comp = new List<Stream[]>();
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users.OrderBy(x => x.Value.Length)) {
                        var stream = hash.Get(user.Key);
                        comp.Add(new[] { new MemoryStream(stream.ReadBytes(stream.Length)), user.Value });
                    }
                });
                Console.WriteLine("Queried {0} users from firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
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
        public void Write_users_with_observable_Firkin() {
            var users = GetDataSource<User>().ToDictionary(k => k.Id, v => GetEntityStream(v));
            if(!users.Any()) {
                return;
            }
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new ObservableFirkinHash<int>(path);
            var observer = Observer.Create<FirkinHashChange<int>>(x => { });
            hash.Subscribe(observer);
            try {
                var elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                Console.WriteLine("Wrote {0} users to firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
            } finally {
                hash.Dispose();
                Directory.Delete(path, true);
            }
        }

        [Test]
        public void Iterate_over_users_with_Firkin() {
            var users = GetDataSource<User>().ToDictionary(k => k.Id, v => GetEntityStream(v));
            if(!users.Any()) {
                return;
            }
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new FirkinHash<int>(path);
            try {
                var elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                Console.WriteLine("Wrote {0} users to firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
                var comp = new List<KeyValuePair<int, Stream>>();
                elapsed = Diagnostics.Time(() => {
                    foreach(var pair in hash) {
                        comp.Add(new KeyValuePair<int, Stream>(pair.Key, new MemoryStream(pair.Value.ReadBytes(pair.Value.Length))));
                    }
                });
                Console.WriteLine("Queried {0} users from firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
                foreach(var pair in comp) {
                    var userStream = users[pair.Key];
                    userStream.Position = 0;
                    Assert.AreEqual(userStream.ReadBytes(userStream.Length), pair.Value.ReadBytes(pair.Value.Length));
                }
            } finally {
                hash.Dispose();
                Directory.Delete(path, true);
            }

        }

        [Test]
        public void Read_write_users_with_hash_reload() {
            var users = GetDataSource<User>().ToDictionary(k => k.Id, v => GetEntityStream(v));
            if(!users.Any()) {
                return;
            }
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var hash = new FirkinHash<int>(path);
            try {
                var elapsed = Diagnostics.Time(() => {
                    foreach(var user in users) {
                        hash.Put(user.Key, user.Value, user.Value.Length);
                    }
                });
                Console.WriteLine("Wrote {0} users to firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
                hash.Dispose();
                _log.DebugFormat("re-loading hash");
                hash = new FirkinHash<int>(path);
                var comp = new List<Stream[]>();
                elapsed = Diagnostics.Time(() => {
                    foreach(var user in users.OrderBy(x => x.Value.Length)) {
                        var stream = hash.Get(user.Key);
                        comp.Add(new[] { new MemoryStream(stream.ReadBytes(stream.Length)), user.Value });
                    }
                });
                Console.WriteLine("Queried {0} users from firkin @ {1:0,0} users/second)", users.Count, users.Count / elapsed.TotalSeconds);
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

        private IEnumerable<T> GetDataSource<T>() {
            var result = new List<T>();
            var datasource = "";
            var t = typeof(T);
            if(t == typeof(Post)) {
                datasource = "posts.xml";
            } else if(t == typeof(User)) {
                datasource = "users.xml";
            } else {
                Console.WriteLine("unrecognized type {0}", t.Name);
                return result;
            }
            datasource = Path.Combine(ConfigurationManager.AppSettings["path.stackoverflow"], datasource);
            if(!File.Exists(datasource)) {
                Console.WriteLine("no such data source: {0}", datasource);
                return result;
            }
            var i = 0;
            var elapsed = Diagnostics.Time(() => {
                foreach(var record in ReadEntitiesFromXml<T>(datasource)) {
                    i++;
                    result.Add(record);
                }
            });
            Console.WriteLine("Read {0} {1} records @ {2:0,0}records/second", i, t.Name, i / elapsed.TotalSeconds);
            return result;
        }

        private Stream GetEntityStream<T>(T entity) {
            var stream = new MemoryStream();
            Serializer.Serialize(stream, entity);
            stream.Position = 0;
            return stream;
        }

        public IEnumerable<T> ReadEntitiesFromXml<T>(string filename) {
            var serializer = new XmlSerializer(typeof(T));
            using(var stream = new FileStream(filename, FileMode.Open)) {
                using(var reader = new XmlTextReader(stream)) {
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
    }
}
