using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public class LockMetadata
    {
        public LockMetadata([NotNull] string lockId, int lockCount, [CanBeNull] string probableOwnerThreadId, long lastWrite)
        {
            LockId = lockId;
            ProbableOwnerThreadId = probableOwnerThreadId;
            LastWrite = lastWrite;
        }

        [NotNull]
        public string LockId { get; }

        /// <summary>
        ///     This is optimization property for long locks.
        ///     Thread that doesn't owns lock tries to get lock periodically.
        ///     Without this property it leads to get_slice operation, which probably leads to scanning tombstones and reading sstables.
        ///     But we can just check is it true that ProbableOwnerThreadId still owns lock and avoid get_slice in many cases.
        /// </summary>
        [CanBeNull]
        public string ProbableOwnerThreadId { get; }

        public long LastWrite { get; }

        public override string ToString()
        {
            return $"LockId: {LockId}, ProbableOwnerThreadId: {ProbableOwnerThreadId}";
        }
    }
}