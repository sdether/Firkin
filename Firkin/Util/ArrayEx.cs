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

namespace Droog.Firkin.Util {
    public static class ArrayEx {
        public static T[] Select<T>(this T[] array, int index, int length) {
            if(index < 0 || index > array.Length - 1) {
                throw new ArgumentOutOfRangeException("index");
            }
            if(length < 0 || index + length > array.Length) {
                throw new ArgumentOutOfRangeException("length");
            }
            var result = new T[length];
            Array.Copy(array, index, result, 0, length);
            return result;
        }

        public static int Compare<T>(this T[] left, T[] right) where T : IComparable<T> {
            if(left == null) {
                throw new ArgumentNullException("left");
            }
            if(right == null) {
                throw new ArgumentNullException("right");
            }
            int result = left.Length - right.Length;
            if(result != 0) {
                return result;
            }
            for(int i = 0; i < left.Length; ++i) {
                result = ((IComparable<T>)left[i]).CompareTo(right[i]);
                if(result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}
