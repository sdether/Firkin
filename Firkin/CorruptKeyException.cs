using System;

namespace Droog.Firkin {
    public class CorruptKeyException : Exception {
        public CorruptKeyException(string error) : base(error) { }
    }
}