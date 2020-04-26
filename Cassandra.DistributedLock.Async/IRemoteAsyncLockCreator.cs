using System.Threading.Tasks;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public interface IRemoteAsyncLockCreator
    {
        [NotNull]
        Task<IRemoteAsyncLock> Lock([NotNull] string lockId);

        [ItemCanBeNull]
        Task<IRemoteAsyncLock> TryGetLock([NotNull] string lockId);
    }
}