using System;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public interface IRemoteAsyncLockImplementation
    {
        TimeSpan KeepLockAliveInterval { get; }

        [ItemNotNull]
        Task<LockAttemptResult> TryLock([NotNull] string lockId, [NotNull] string threadId);

        Task<bool> TryUnlock([NotNull] string lockId, [NotNull] string threadId);
        Task<bool> TryRelock([NotNull] string lockId, [NotNull] string threadId);
    }
}