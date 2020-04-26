using System;
using System.Threading.Tasks;

using JetBrains.Annotations;

using SkbKontur.Cassandra.DistributedLock.Async.Cluster;

namespace SkbKontur.Cassandra.DistributedLock.Async.LightweightTransactions
{
    public class LightweightTransactionsAsyncLockImplementation : IRemoteAsyncLockImplementation
    {
        public LightweightTransactionsAsyncLockImplementation(
            ICassandraCluster cassandraCluster,
            LightweightTransactionsAsyncLockImplementationSettings settings)
        {
            lockTtl = settings.LockTtl;
            KeepLockAliveInterval = settings.KeepLockAliveInterval;
            baseAsyncOperationsPerformer = new LightweightTransactionsAsyncLockOperationsPerformer(cassandraCluster, settings);
        }

        public TimeSpan KeepLockAliveInterval { get; }

        [ItemNotNull]
        public async Task<LockAttemptResult> TryLock([NotNull] string lockId, [NotNull] string threadId)
        {
            var aliveLockThread = await baseAsyncOperationsPerformer.GetLockAliveThread(lockId).ConfigureAwait(false);

            if (!string.IsNullOrEmpty(aliveLockThread))
            {
                if (aliveLockThread == threadId)
                    throw new InvalidOperationException($"TryLock(lockId = {lockId}, threadId = {threadId}): probableOwnerThreadId == threadId, though it seemed to be impossible!");
                return LockAttemptResult.AnotherOwner(aliveLockThread);
            }
            return await RunBattle(lockId, threadId).ConfigureAwait(false);
        }

        [NotNull]
        private async Task<LockAttemptResult> RunBattle([NotNull] string lockId, [NotNull] string threadId)
        {
            var (isLockApplied, lockMetadata) = await baseAsyncOperationsPerformer.TryWriteThread(lockId, threadId, lockTtl);
            if (isLockApplied)
            {
                return LockAttemptResult.Success();
            }

            if (lockMetadata == null)
            {
                var existingLock = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId);
                if (existingLock == null || string.IsNullOrEmpty(existingLock.ProbableOwnerThreadId))
                {
                    return LockAttemptResult.ConcurrentAttempt();
                }
                return existingLock.ProbableOwnerThreadId == threadId
                    ? LockAttemptResult.Success()
                    : LockAttemptResult.AnotherOwner(existingLock.ProbableOwnerThreadId);
            }

            return string.IsNullOrEmpty(lockMetadata.ProbableOwnerThreadId)
                ? LockAttemptResult.ConcurrentAttempt()
                : LockAttemptResult.AnotherOwner(lockMetadata.ProbableOwnerThreadId);
        }

        public async Task<bool> TryUnlock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            if (lockMetadata == null)
                return false;
            return await baseAsyncOperationsPerformer.DeleteThread(lockId, threadId);
        }

        public async Task<bool> TryRelock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            if (lockMetadata == null)
                return false;
            return await baseAsyncOperationsPerformer.TryRenewThread(lockId, threadId, lockTtl).ConfigureAwait(false);
        }

        [ItemCanBeNull]
        public async Task<LockMetadata> GetLockMetadata([NotNull] string lockId)
        {
            var lockMetadata = await baseAsyncOperationsPerformer.TryGetLockMetadata(lockId).ConfigureAwait(false);
            return lockMetadata;
        }

        private readonly TimeSpan lockTtl;
        private readonly LightweightTransactionsAsyncLockOperationsPerformer baseAsyncOperationsPerformer;
    }
}