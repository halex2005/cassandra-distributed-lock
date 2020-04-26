using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public class NewLockMetadata
    {
        public NewLockMetadata([NotNull] string lockId, [NotNull] string ownerThreadId, long lastWriteTicks)
        {
            LockId = lockId;
            OwnerThreadId = ownerThreadId;
            LastWriteTicks = lastWriteTicks;
        }

        [NotNull]
        public string LockId { get; }

        [NotNull]
        public string OwnerThreadId { get; }

        public long LastWriteTicks { get; }

        public override string ToString()
        {
            return $"LockId: {LockId}, OwnerThreadId: {OwnerThreadId}";
        }
    }
}