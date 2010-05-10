using System;
using System.IO;

namespace Droog.Firkin.Serialization {
    public class StreamSerializationLambdaWrapper<TValue> : IStreamSerializer<TValue> {
        public Action<Stream, TValue> Serializer;
        public Func<Stream, TValue> Deserializer;

        public void Serialize(Stream destination, TValue value) {
            Serializer(destination, value);
        }

        public TValue Deserialize(Stream source) {
            return Deserializer(source);
        }
    }
}