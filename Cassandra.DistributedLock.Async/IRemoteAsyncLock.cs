using System;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public interface IRemoteAsyncLock : IAsyncDisposable
    {
        string LockId { get; }
        string ThreadId { get; }
    }
}