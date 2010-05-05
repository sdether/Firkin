using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using ProtoBuf;

namespace Droog.Firkin.Test.Perf.Stackoverflow {
    [ProtoContract]
    [XmlRoot("row")]
    public class User {
        [ProtoMember(1)]
        [XmlAttribute]
        public int Id;
        [ProtoMember(2)]
        [XmlAttribute]
        public int Reputation;
        [ProtoMember(3)]
        [XmlAttribute]
        public DateTime CreationDate;
        [ProtoMember(4)]
        [XmlAttribute]
        public string DisplayName;
        [ProtoMember(5)]
        [XmlAttribute]
        public string EmailHash;
        [ProtoMember(6)]
        [XmlAttribute]
        public DateTime LastAccessDate;
        [ProtoMember(7)]
        [XmlAttribute]
        public string WebsiteUrl;
        [ProtoMember(8)]
        [XmlAttribute]
        public string Location;
        [ProtoMember(9)]
        [XmlAttribute]
        public int Age;
        [ProtoMember(10)]
        [XmlAttribute]
        public string AboutMe;
        [ProtoMember(11)]
        [XmlAttribute]
        public int Views;
        [ProtoMember(12)]
        [XmlAttribute]
        public int UpVotes;
        [ProtoMember(13)]
        [XmlAttribute]
        public int DownVotes;
    }
}
