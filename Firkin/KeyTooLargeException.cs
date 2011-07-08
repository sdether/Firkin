using System;

namespace Droog.Firkin {
    public class KeyTooLargeException : Exception {
        public KeyTooLargeException(long actual, uint maxKeySize) {
        }
    }
}