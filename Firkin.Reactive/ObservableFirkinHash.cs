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
using System.Linq;
using System.Text;
using Droog.Firkin;
using Droog.Firkin.Serialization;

namespace Firkin.Reactive {
    public class ObservableFirkinHash<TKey> : FirkinHash<TKey>, IObservableFirkinHash<TKey> {

        //--- Fields ---
        private readonly Dictionary<int, IObserver<FirkinHashChange<TKey>>> _subscribers = new Dictionary<int, IObserver<FirkinHashChange<TKey>>>();
        private int _subscriberKey;

        //--- Constructors ---
        public ObservableFirkinHash(string storeDirectory)
            : base(storeDirectory) {
            Init();
        }

        public ObservableFirkinHash(string storeDirectory, long maxFileSize)
            : base(storeDirectory, maxFileSize) {
            Init();
        }
        public ObservableFirkinHash(string storeDirectory, long maxFileSize, IByteArraySerializer<TKey> serializer)
            : base(storeDirectory, maxFileSize, serializer) {
            Init();
        }

        //--- Methods ---
        public IDisposable Subscribe(IObserver<FirkinHashChange<TKey>> observer) {
            CheckDisposed();
            if(observer == null) {
                throw new ArgumentNullException("observer");
            }
            lock(_indexSyncRoot) {
                int k = _subscriberKey++;
                _subscribers[k] = observer;
                return new DisposableClosure(() => {
                    lock(_indexSyncRoot) {
                        _subscribers.Remove(k);
                    }
                });
            }
        }

        protected override void Dispose(bool disposing) {
            if(disposing) {
                OnCompleted();
            }
            base.Dispose(disposing);
        }

        private void Init() {
            _changeObserver = OnNext;
        }

        private void OnNext(FirkinHashChange<TKey> value) {
            CheckDisposed();
            foreach(IObserver<FirkinHashChange<TKey>> observer in _subscribers.Select(kv => kv.Value)) {
                observer.OnNext(value);
            }
        }

        private void OnError(Exception exception) {
            CheckDisposed();
            if(exception == null) {
                throw new ArgumentNullException("exception");
            }
            foreach(var observer in _subscribers.Select(kv => kv.Value)) {
                observer.OnError(exception);
            }
        }

        private void OnCompleted() {
            CheckDisposed();
            foreach(var observer in _subscribers.Select(kv => kv.Value)) {
                observer.OnCompleted();
            }
        }
    }

    public interface IObservableFirkinHash<TKey> : IObservable<FirkinHashChange<TKey>>, IFirkinHash<TKey> { }

    class DisposableClosure : IDisposable {
        Action dispose;
        public DisposableClosure(Action dispose) {
            this.dispose = dispose;
        }

        public void Dispose() {
            dispose();
        }
    }


}
