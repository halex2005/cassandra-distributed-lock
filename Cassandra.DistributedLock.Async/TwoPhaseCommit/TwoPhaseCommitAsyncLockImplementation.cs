using System;
using System.Linq;
using System.Threading.Tasks;

using JetBrains.Annotations;

using SkbKontur.Cassandra.DistributedLock.Async.Cluster;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit
{
    public class TwoPhaseCommitAsyncLockImplementation : IRemoteAsyncLockImplementation
    {
        public TwoPhaseCommitAsyncLockImplementation(
            ICassandraCluster cassandraCluster,
            TwoPhaseCommitAsyncLockImplementationSettings settings)
        {
            lockTtl = settings.LockTtl;
            KeepLockAliveInterval = settings.KeepLockAliveInterval;
            baseAsyncOperationsPerformer = new TwoPhaseCommitAsyncLockOperationsPerformer(cassandraCluster, settings);
        }

        public TimeSpan KeepLockAliveInterval { get; }

        [ItemNotNull]
        public async Task<LockAttemptResult> TryLock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId)
                ?? new LockMetadata(lockId, 0, null, PreciseTimestampGenerator.Instance.NowTicks());

            var probableOwnerThreadId = lockMetadata.ProbableOwnerThreadId;
            if (!string.IsNullOrEmpty(probableOwnerThreadId)
                && await baseAsyncOperationsPerformer.ThreadAlive(lockMetadata.LockId, probableOwnerThreadId).ConfigureAwait(false))
            {
                if (probableOwnerThreadId == threadId)
                    throw new InvalidOperationException($"TryLock(lockId = {lockId}, threadId = {threadId}): probableOwnerThreadId == threadId, though it seemed to be impossible!");
                return LockAttemptResult.AnotherOwner(probableOwnerThreadId);
            }
            return await RunBattle(lockMetadata, threadId).ConfigureAwait(false);
        }

        [NotNull]
        private async Task<LockAttemptResult> RunBattle([NotNull] LockMetadata lockMetadata, [NotNull] string threadId)
        {
            var items = await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Main).ConfigureAwait(false);
            if (items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if (items.Length > 1)
            {
                if (items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }
            var beforeOurWriteShades = await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Shadow).ConfigureAwait(false);
            if (beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            await baseAsyncOperationsPerformer.WriteThread(lockMetadata.LockId, threadId, LockStatus.Shadow, lockTtl).ConfigureAwait(false);
            var shades = await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Shadow).ConfigureAwait(false);
            if (shades.Length == 1)
            {
                items = await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Main).ConfigureAwait(false);
                if (items.Length == 0)
                {
                    await baseAsyncOperationsPerformer.WriteThread(lockMetadata.LockId, threadId, LockStatus.Main, lockTtl).ConfigureAwait(false);
                    return LockAttemptResult.Success();
                }
            }
            await baseAsyncOperationsPerformer.DeleteThread(lockMetadata.LockId, threadId).ConfigureAwait(false);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public async Task<bool> TryUnlock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            if (lockMetadata == null)
                return false;
            await baseAsyncOperationsPerformer.DeleteThread(lockMetadata.LockId, threadId);
            return true;
        }

        public async Task<bool> TryRelock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            if (lockMetadata == null)
                return false;
            await baseAsyncOperationsPerformer.WriteThread(lockId, threadId, LockStatus.Main, lockTtl).ConfigureAwait(false);
            return true;
        }

        [ItemNotNull]
        public async Task<string[]> GetLockThreads([NotNull] string lockId)
        {
            var lockMetadata = await GetLockMetadata(lockId).ConfigureAwait(false);
            return await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Main).ConfigureAwait(false);
        }

        [ItemNotNull]
        public async Task<string[]> GetShadeThreads([NotNull] string lockId)
        {
            var lockMetadata = await GetLockMetadata(lockId).ConfigureAwait(false);
            return await baseAsyncOperationsPerformer.SearchThreads(lockMetadata.LockId, LockStatus.Shadow).ConfigureAwait(false);
        }

        [ItemNotNull]
        public async Task<LockMetadata> GetLockMetadata([NotNull] string lockId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            if (lockMetadata == null)
                throw new InvalidOperationException($"Not found metadata for lockId = {lockId}");
            return lockMetadata;
        }

        private readonly TimeSpan lockTtl;
        private readonly TwoPhaseCommitAsyncLockOperationsPerformer baseAsyncOperationsPerformer;
    }
}