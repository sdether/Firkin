using System;

namespace Droog.Firkin.Serialization {
    public class ByteArraySerializationLambdaWrapper<TKey> : IByteArraySerializer<TKey> {
        public Func<byte[], TKey> Deserializer;
        public Func<TKey, byte[]> Serializer;
        public byte[] Serialize(TKey key) {
            return Serializer(key);
        }

        public TKey Deserialize(byte[] bytes) {
            return Deserializer(bytes);
        }
    }
}