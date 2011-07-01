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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Droog.Firkin.Data;
using Droog.Firkin.IO;
using Droog.Firkin.Serialization;
using log4net;

namespace Droog.Firkin {
    public class FirkinHash<TKey> : IFirkinHash<TKey> {

        //--- Constants ---
        public const long DEFAULT_MAX_FILE_SIZE = 10 * 1024 * 1024;
        private const string STORAGE_FILE_PREFIX = "store_";
        private const string MERGE_FILE_PREFIX = "merge_";
        private const string OLD_FILE_PREFIX = "old_";
        private const string DATA_FILE_EXTENSION = ".data";
        private const string HINT_FILE_EXTENSION = ".hint";

        //--- Types ---
        private class MergePair {
            public IFirkinActiveFile Data;
            public IFirkinHintFile Hint;
        }

        //--- Class Fields ---
        protected static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        //--- Fields ---
        private readonly long _maxFileSize;
        private readonly string _storeDirectory;
        private readonly IByteArraySerializer<TKey> _serializer;
        protected readonly object _mergeSyncRoot = new object();
        protected readonly object _indexSyncRoot = new object();

        private Dictionary<TKey, KeyInfo> _index = new Dictionary<TKey, KeyInfo>();
        private Dictionary<ushort, IFirkinFile> _files = new Dictionary<ushort, IFirkinFile>();
        private IFirkinActiveFile _head;
        protected bool _isDisposed;
        protected Action<FirkinHashChange<TKey>> _changeObserver;

        //--- Constructors ---
        public FirkinHash(string storeDirectory) : this(storeDirectory, DEFAULT_MAX_FILE_SIZE) { }
        public FirkinHash(string storeDirectory, long maxFileSize) : this(storeDirectory, maxFileSize, null) { }

        public FirkinHash(string storeDirectory, long maxFileSize, IByteArraySerializer<TKey> serializer) {
            if(string.IsNullOrEmpty(storeDirectory)) {
                throw new ArgumentNullException("storeDirectory");
            }
            _storeDirectory = storeDirectory;
            _maxFileSize = maxFileSize;
            if(serializer == null) {
                try {
                    serializer = SerializerRepository.GetByteArraySerializer<TKey>();
                } catch(Exception e) {
                    throw new ArgumentException(string.Format("Cannot serialize generic parameter '{0}' without an appropriate IByteArraySerializer", typeof(TKey)), e);
                }
            }
            _serializer = serializer;
            Initialize();
        }

        //--- Properties ---
        public int Count { get { return _index.Count; } }
        public IEnumerable<TKey> Keys { get { lock(_indexSyncRoot) { return _index.Keys.ToArray(); } } }

        //--- Methods ---
        public void Put(TKey key, Stream stream) {
            Put(key, stream, stream.Length);
        }

        public void Put(TKey key, Stream stream, uint length) {
            if(length == 0) {
                Delete(key);
                return;
            }
            var action = FirkinHashChangeAction.Add;
            lock(_indexSyncRoot) {
                var keyInfo = _head.Write(new KeyValuePair() {
                    Key = _serializer.Serialize(key),
                    Value = stream,
                    ValueSize = length
                });
                if(_changeObserver != null) {
                    action = _index.ContainsKey(key) ? FirkinHashChangeAction.Change : FirkinHashChangeAction.Add;
                }
                _index[key] = keyInfo;
                CheckHead();
            }
            if(_changeObserver != null) {
                _changeObserver(new FirkinHashChange<TKey>(key, action));
            }
        }

        public void Put(TKey key, Stream stream, long length) {
            Put(key, stream, (uint)length);
        }

        public void Flush() {
            lock(_indexSyncRoot) {
                foreach(var file in _files.Values) {
                    file.Flush();
                }
            }
        }

        public FirkinStream Get(TKey key) {
            KeyInfo info;
            return !_index.TryGetValue(key, out info) ? null : _files[info.FileId].ReadValue(info);
        }

        public void Merge() {

            lock(_mergeSyncRoot) {
                IFirkinFile[] oldFiles;
                IFirkinFile head;
                lock(_indexSyncRoot) {
                    head = _head;
                    oldFiles = _files.Values.Where(x => x != head).OrderBy(x => x.FileId).ToArray();
                }
                _log.DebugFormat("starting merge of {0} files (with head at id {1}) in '{2}' ", oldFiles.Length, head.FileId, _storeDirectory);
                if(oldFiles.Length == 0) {

                    // not merging if there is only one archive file
                    return;
                }

                // merge current data into new data files and write out accompanying hint files
                ushort fileId = 0;
                var mergePairs = new List<MergePair>();
                MergePair current = null;
                uint serial = 0;
                foreach(var file in oldFiles) {
                    var deleted = 0;
                    var outofdate = 0;
                    var active = 0;
                    foreach(var record in file.GetRecords()) {
                        if(current == null) {
                            fileId++;
                            serial = 0;
                            current = new MergePair() {
                                Data = FirkinFile.CreateActive(GetMergeDataFilename(fileId), fileId),
                                Hint = new FirkinHintFile(GetMergeHintFilename(fileId))
                            };
                            mergePairs.Add(current);
                        }
                        if(record.ValueSize == 0) {

                            // not including deletes on merge
                            deleted++;
                            continue;
                        }
                        var key = _serializer.Deserialize(record.Key);

                        // TODO: do i need a lock on _index here?
                        KeyInfo info;
                        if(!_index.TryGetValue(key, out info)) {

                            // not including record that's no longer in index
                            outofdate++;
                            continue;
                        }
                        if(info.FileId != file.FileId || info.Serial != record.Serial) {

                            // not including out-of-date record
                            outofdate++;
                            continue;
                        }
                        var newRecord = record;
                        newRecord.Serial = ++serial;
                        var valuePosition = current.Data.Write(newRecord);
                        current.Hint.WriteHint(newRecord, valuePosition);

                        // if our current file is over the maxsize and not about to collide with the head's id ...
                        if(current.Data.Size > _maxFileSize && fileId < head.FileId) {

                            // ... set it to null, so we can create the next file
                            current = null;
                        }
                        active++;
                    }
                    _log.DebugFormat("read {0} records, skipped {1} deleted and {2} outofdate", active, deleted, outofdate);
                }
                _log.DebugFormat("merged {0} file(s) into {1} file(s)", oldFiles.Length, mergePairs.Count);

                // rebuild the index based on new files
                var newIndex = new Dictionary<TKey, KeyInfo>();
                var newFiles = new Dictionary<ushort, IFirkinFile>();
                var mergeFiles = new List<IFirkinFile>();
                var mergedRecords = 0;
                foreach(var pair in mergePairs) {
                    var file = FirkinFile.OpenArchiveFromActive(pair.Data);
                    newFiles.Add(file.FileId, file);
                    mergeFiles.Add(file);
                    foreach(var hint in pair.Hint) {
                        var keyInfo = new KeyInfo(pair.Data.FileId, hint);
                        var key = _serializer.Deserialize(hint.Key);
                        newIndex[key] = keyInfo;
                        mergedRecords++;
                    }
                    pair.Hint.Dispose();
                }
                _log.DebugFormat("read {0} records from hint files", mergedRecords);

                // add records && files not part of merge
                lock(_indexSyncRoot) {
                    foreach(var file in _files.Values.Where(x => x.FileId >= head.FileId).OrderBy(x => x.FileId)) {
                        newFiles[file.FileId] = file;
                        foreach(var pair in file) {
                            var key = _serializer.Deserialize(pair.Key);
                            if(pair.Value.ValueSize == 0) {
                                newIndex.Remove(key);
                            } else {
                                newIndex[key] = pair.Value;
                            }
                        }
                        _log.DebugFormat("added entries from file {0}: {1}", file.FileId, newIndex.Count);
                    }

                    // swap out index and file list
                    _index = newIndex;
                    _files = newFiles;
                }
                try {

                    // move old files out of the way
                    foreach(var file in oldFiles) {
                        file.Dispose();
                        var oldFile = GetOldDataFilename(file.FileId);
#if DEBUG
                        _log.DebugFormat("moving old from {0} to {1}", Path.GetFileName(file.Filename), Path.GetFileName(oldFile));
#endif
                        File.Move(file.Filename, oldFile);
                        var hintfile = GetHintFilename(file.FileId);
                        if(File.Exists(hintfile)) {
                            var oldHintFile = GetOldHintFilename(file.FileId);
#if DEBUG
                            _log.DebugFormat("moving old hint from {0} to {1}", Path.GetFileName(hintfile), Path.GetFileName(oldHintFile));
#endif
                            File.Move(hintfile, oldHintFile);
                        }
                    }

                    // move new files into place
                    foreach(var file in mergeFiles) {
#if DEBUG
                        _log.DebugFormat("creating file and hint for id {0}", file.FileId);
#endif
                        file.Rename(GetDataFilename(file.FileId));
                        File.Move(GetMergeHintFilename(file.FileId), GetHintFilename(file.FileId));
                    }

                    // delete old files
                    foreach(var file in oldFiles) {
                        var oldFile = GetOldDataFilename(file.FileId);
#if DEBUG
                        _log.DebugFormat("deleting old file {0}", Path.GetFileName(oldFile));
#endif
                        File.Delete(oldFile);
                        var hintfile = GetOldHintFilename(file.FileId);
                        if(File.Exists(hintfile)) {
#if DEBUG
                            _log.DebugFormat("deleting old hint file {0}", Path.GetFileName(hintfile));
#endif
                            File.Delete(hintfile);
                        }
                    }
                } catch(Exception e) {

                    // something went wrong, try to recover to pre-merge state
                    // TODO: go back to pre-merge state
                    _log.Warn("Unable to complete merge", e);
                }
            }
            _log.DebugFormat("completed merge in '{0}'", _storeDirectory);
        }

        public bool Delete(TKey key) {
            lock(_indexSyncRoot) {
                KeyInfo info;
                if(!_index.TryGetValue(key, out info)) {
                    return false;
                }
                _index.Remove(key);
                _head.Write(new KeyValuePair() {
                    Key = _serializer.Serialize(key),
                    Value = null,
                    ValueSize = 0
                });

                // zero out the value size, so that an iterator can recognize the info as deleted
                info.ValueSize = 0;
                CheckHead();
            }
            if(_changeObserver != null) {
                _changeObserver(new FirkinHashChange<TKey>(key, FirkinHashChangeAction.Delete));
            }
            return true;
        }

        public IEnumerator<KeyValuePair<TKey, FirkinStream>> GetEnumerator() {
            KeyValuePair<TKey, KeyInfo>[] pairs;
            lock(_indexSyncRoot) {
                pairs = _index.ToArray();
            }
            foreach(var pair in pairs) {
                IFirkinFile file;
                FirkinStream stream = null;
                if(pair.Value.ValueSize == 0) {

                    // key has been deleted, skip it
                    continue;
                }
                try {
                    stream = _files[pair.Value.FileId].ReadValue(pair.Value);
                } catch {

                    // this may fail, and that's fine, just means we may have to degrade to Get() call
                }
                if(stream == null) {
                    if(pair.Value.ValueSize == 0) {

                        // key was deleted while we tried to get it, skip it
                        continue;
                    }

                    // try to get the key via Get()
                    stream = Get(pair.Key);
                }
                if(stream != null) {
                    yield return new KeyValuePair<TKey, FirkinStream>(pair.Key, stream);
                }
            }
        }

        public void Truncate() {

            // Note: Have to acquire merge then index syncroots otherwise we're liable to run into a deadlock
            lock(_mergeSyncRoot) {
                lock(_indexSyncRoot) {
                    foreach(var file in _files.Values) {
                        var filename = file.Filename;
                        file.Dispose();
                        File.Delete(filename);
                    }
                    _files.Clear();
                    _index.Clear();
                    _head = FirkinFile.CreateActive(GetDataFilename(1), 1);
                    _files[_head.FileId] = _head;
                }
            }
        }

        public void Dispose() {
            if(_isDisposed) {
                return;
            }
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing) {
            foreach(var file in _files.Values) {
                file.Dispose();
            }
        }

        protected void CheckDisposed() {
            if(_isDisposed) {
                throw new ObjectDisposedException(this.GetType().ToString());
            }
        }

        private void CheckHead() {
            if(_head.Size < _maxFileSize) {
                return;
            }
            NewHead();
        }

        private void NewHead() {
            _log.DebugFormat("switching to new active file at size {0}", _head.Size);
            _files[_head.FileId] = FirkinFile.OpenArchiveFromActive(_head);
            var fileId = (ushort)(_head.FileId + 1);
            _head = FirkinFile.CreateActive(GetDataFilename(fileId), fileId);
            _files[_head.FileId] = _head;
        }

        private void Initialize() {
            if(!Directory.Exists(_storeDirectory)) {
                Directory.CreateDirectory(_storeDirectory);
            }

            // get all data files
            var files = from filename in Directory.GetFiles(_storeDirectory, STORAGE_FILE_PREFIX + "*" + DATA_FILE_EXTENSION)
                        let fileId = ParseFileId(filename)
                        orderby fileId
                        select new { FileId = fileId, Filename = filename };
            uint maxSerial = 0;
            IFirkinArchiveFile last = null;
            foreach(var fileInfo in files) {
                maxSerial = 0;
                var file = FirkinFile.OpenArchive(fileInfo.Filename, fileInfo.FileId);
                _files.Add(file.FileId, file);

                // iterate over key info
                var hintFilename = GetHintFilename(fileInfo.FileId);
                var count = 0;
                var delete = 0;
                if(File.Exists(hintFilename)) {
                    var hintFile = new FirkinHintFile(hintFilename);
                    foreach(var hint in hintFile) {
                        var keyInfo = new KeyInfo(fileInfo.FileId, hint);
                        var key = _serializer.Deserialize(hint.Key);
                        _index[key] = keyInfo;
                        count++;
                    }
                    hintFile.Dispose();
                    _log.DebugFormat("read {0} record markers from hint file {1}", count, fileInfo.FileId);
                } else {
                    foreach(var pair in file) {
                        var key = _serializer.Deserialize(pair.Key);
                        maxSerial = pair.Value.Serial;
                        if(pair.Value.ValueSize == 0) {
                            _index.Remove(key);
                            delete++;
                        } else {
                            _index[key] = pair.Value;
                            count++;
                        }
                    }
                    _log.DebugFormat("read {0} record and {1} delete markers from data file {2}", count, delete, fileInfo.FileId);
                }
                last = file;
            }
            if(last != null && last.Size < _maxFileSize) {
                _head = FirkinFile.OpenActiveFromArchive(last, maxSerial);
            } else {
                ushort fileId = 1;
                if(last != null) {
                    fileId += last.FileId;
                }
                _head = FirkinFile.CreateActive(GetDataFilename(fileId), fileId);
            }
            _files[_head.FileId] = _head;
        }

        private string GetMergeDataFilename(ushort fileId) {
            return GetFilename(fileId, MERGE_FILE_PREFIX, DATA_FILE_EXTENSION);
        }

        private string GetMergeHintFilename(ushort fileId) {
            return GetFilename(fileId, MERGE_FILE_PREFIX, HINT_FILE_EXTENSION);
        }

        private string GetDataFilename(ushort fileId) {
            return GetFilename(fileId, STORAGE_FILE_PREFIX, DATA_FILE_EXTENSION);
        }

        private string GetHintFilename(ushort fileId) {
            return GetFilename(fileId, STORAGE_FILE_PREFIX, HINT_FILE_EXTENSION);
        }

        private string GetOldDataFilename(ushort fileId) {
            return GetFilename(fileId, OLD_FILE_PREFIX, DATA_FILE_EXTENSION);
        }

        private string GetOldHintFilename(ushort fileId) {
            return GetFilename(fileId, OLD_FILE_PREFIX, HINT_FILE_EXTENSION);
        }

        private string GetFilename(ushort fileId, string prefix, string extension) {
            return Path.Combine(_storeDirectory, prefix + fileId + extension);
        }

        private ushort ParseFileId(string filename) {
            var idString = Path.GetFileNameWithoutExtension(filename).Remove(0, STORAGE_FILE_PREFIX.Length);
            ushort fileId;
            if(!ushort.TryParse(idString, out fileId)) {
                throw new ArgumentException();
            }
            return fileId;
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }
    }
}