using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Droog.Firkin.Test.Perf.Stackoverflow {
    [XmlRoot("row")]
    public class Post {
        [XmlAttribute]
        public int Id;
        [XmlAttribute]
        public PostType PostType;
        [XmlAttribute]
        public int ParentId;
        [XmlAttribute]
        public int AcceptedAnswerId;
        [XmlAttribute]
        public DateTime CreationDate;
        [XmlAttribute]
        public int Score;
        [XmlAttribute]
        public int ViewCount;
        [XmlAttribute]
        public string Body;
        [XmlAttribute]
        public int OwnerUserId;
        [XmlAttribute]
        public int LastEditorUserId;
        [XmlAttribute]
        public string LastEditorDisplayname;
        [XmlAttribute]
        public DateTime LastEditDate;
        [XmlAttribute]
        public DateTime LastActivityDate;
        [XmlAttribute]
        public DateTime CommunityOwnedDate;
        [XmlAttribute]
        public DateTime ClosedDate;
        [XmlAttribute]
        public string Title;
        [XmlAttribute]
        public string Tags;
        [XmlAttribute]
        public int AnswerCount;
        [XmlAttribute]
        public int CommentCount;
        [XmlAttribute]
        public int FavoriteCount;
    }

    public enum PostType {
        Question = 1,
        Answer = 2
    }
}
