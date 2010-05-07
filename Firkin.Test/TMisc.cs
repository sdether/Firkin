using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Droog.Firkin.Test {
    
    [TestFixture]
    public class TMisc {

        [Test]
        public void Dictionary_kvp_Contains_checks_key_and_value() {
            IDictionary<int,string> dictionary = new Dictionary<int, string>();
            dictionary[1] = "foo";
            Assert.IsTrue(dictionary.Contains(new KeyValuePair<int, string>(1, "foo")));
            Assert.IsFalse(dictionary.Contains(new KeyValuePair<int, string>(1, "bar")));
        }

        [Test]
        public void Dictionary_kvp_Remove_checks_key_and_value() {
            IDictionary<int, string> dictionary = new Dictionary<int, string>();
            dictionary[1] = "foo";
            Assert.IsFalse(dictionary.Remove(new KeyValuePair<int, string>(1, "bar")));
            Assert.IsTrue(dictionary.Remove(new KeyValuePair<int, string>(1, "foo")));
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void Dictionary_CopyTo_requires_destination_of_sufficient_size() {
            IDictionary<int, string> dictionary = new Dictionary<int, string>();
            dictionary[1] = "foo";
            dictionary[2] = "foo";
            dictionary[3] = "foo";
            var destination = new KeyValuePair<int, string>[2];
            dictionary.CopyTo(destination, 0);
        }
    }
}
