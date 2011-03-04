using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using NUnit.Framework;

namespace Droog.Firkin.Test.Perf {

    [TestFixture]
    public class TAsync {

        [Test]
        public void Fire_off_lots_of_little_tasks() {
            var n = 1000000;
            var mres = new List<ManualResetEvent>();
            for(var i = 0; i < n; i++) {
                mres.Add(new ManualResetEvent(false));
            }
            var t = Stopwatch.StartNew();
            foreach(var x in mres) {
                var mre = x;
                Observable.Start(() => mre.Set());
            }
            foreach(var x in mres) {
                x.WaitOne();
            }
            t.Stop();
            Console.WriteLine("fired off {0} tasks in {1}ms ({2:0} tasks/second)",n,t.ElapsedMilliseconds,n/t.Elapsed.TotalSeconds);
        }
    }
}
