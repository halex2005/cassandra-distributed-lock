using System.Threading.Tasks;

namespace SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker
{
    public class RemoteAsyncLockHandle : IRemoteAsyncLock
    {
        public RemoteAsyncLockHandle(string lockId, string threadId, RemoteAsyncLocker remoteAsyncLocker)
        {
            LockId = lockId;
            ThreadId = threadId;
            this.remoteAsyncLocker = remoteAsyncLocker;
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask(remoteAsyncLocker.ReleaseLock(LockId, ThreadId));
        }

        public string LockId { get; }
        public string ThreadId { get; }

        private readonly RemoteAsyncLocker remoteAsyncLocker;
    }
}