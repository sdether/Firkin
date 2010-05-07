using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Droog.Firkin.Test.Perf.Stackoverflow {
    public class Votes {
        public int Id;
        public int PostId;
        public VoteType VoteType;
        public DateTime CreationDate;
        public int UserId;
        public int BountyAmount;
    }

    public enum VoteType {
        AcceptedByOriginator = 1,
        UpMod = 2,
        DownMod = 3,
        Offensive = 4,
        Favorite = 5,
        Close = 6,
        Reopen = 7,
        BountyStart = 8,
        BountyClose = 9,
        Deletion = 10,
        Undeletion = 11,
        Spam = 12,
        InformModerator = 13,
    }
}
