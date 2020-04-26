using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using SkbKontur.Cassandra.TimeBasedUuid;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker
{
    public class RemoteAsyncLocker : IRemoteAsyncLockCreator, IAsyncDisposable
    {
        public RemoteAsyncLocker(IRemoteAsyncLockImplementation remoteAsyncLockImplementation, RemoteAsyncLockerMetrics metrics, ILog logger)
        {
            this.remoteAsyncLockImplementation = remoteAsyncLockImplementation;
            this.metrics = metrics;
            this.logger = logger.ForContext("CassandraDistributedLock");
            keepLockAliveInterval = remoteAsyncLockImplementation.KeepLockAliveInterval;
            lockOperationWarnThreshold = remoteAsyncLockImplementation.KeepLockAliveInterval.Multiply(2);
            remoteLocksKeeperThread = new Thread(KeepRemoteLocksAlive)
                {
                    IsBackground = true,
                    Name = "remoteLocksKeeper",
                };
            remoteLocksKeeperThread.Start();
        }

        [ItemNotNull]
        public async Task<IRemoteAsyncLock> Lock([System.Diagnostics.CodeAnalysis.NotNull] string lockId)
        {
            var threadId = Guid.NewGuid().ToString();

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.For("Lock").Increment();
                logger.Error("Lock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.LockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
            {
                while (true)
                {
                    var acquireLockResult = await TryAcquireLock(lockId, threadId).ConfigureAwait(false);
                    if (acquireLockResult.Lock != null)
                        return acquireLockResult.Lock;
                    var longSleep = ThreadLocalRandom.Instance.Next(1000);
                    logger.Warn("Поток {0} не смог взять блокировку {1}, потому что поток {2} владеет ей в данный момент. Засыпаем на {3} миллисекунд.", threadId, lockId, acquireLockResult.ConcurrentThreadId, longSleep);
                    await Task.Delay(longSleep).ConfigureAwait(false);
                }
            }
        }

        [ItemCanBeNull]
        public async Task<IRemoteAsyncLock> TryGetLock([System.Diagnostics.CodeAnalysis.NotNull] string lockId)
        {
            var threadId = Guid.NewGuid().ToString();

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.For("TryGetLock").Increment();
                logger.Error("TryGetLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.TryGetLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
            {
                var acquireLockResult = await TryAcquireLock(lockId, threadId).ConfigureAwait(false);
                return acquireLockResult.Lock;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (isDisposed)
                return;
            try
            {
                remoteLocksQueue.CompleteAdding();
                var waitRemoteLocksKeeperThreadToEnd = new Task(() => remoteLocksKeeperThread.Join(), TaskCreationOptions.LongRunning);
                waitRemoteLocksKeeperThreadToEnd.Start();
                await waitRemoteLocksKeeperThreadToEnd;
                remoteLocksQueue.Dispose();
                isDisposed = true;
            }
            catch
            {
                // ignore
            }
        }

        private Task<(IRemoteAsyncLock Lock, string ConcurrentThreadId)> TryAcquireLock(string lockId, string threadId)
        {
            EnsureNotDisposed();
            ValidateArgs(lockId, threadId);

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.For("TryAcquireLock").Increment();
                logger.Error("TryAcquireLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.TryAcquireLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
                return DoTryAcquireLock(lockId, threadId);
        }

        public Task ReleaseLock(string lockId, string threadId)
        {
            EnsureNotDisposed();
            ValidateArgs(lockId, threadId);

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.For("ReleaseLock").Increment();
                logger.Error("ReleaseLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.ReleaseLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
                return DoReleaseLock(lockId, threadId);
        }

        private static string FormatLockOperationId(string lockId, string threadId)
        {
            return $"lockId: {lockId}, threadId: {threadId}";
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
        private static void ValidateArgs(string lockId, string threadId)
        {
            if (string.IsNullOrEmpty(lockId))
                throw new InvalidOperationException("lockId is empty");
            if (string.IsNullOrEmpty(threadId))
                throw new InvalidOperationException("threadId is empty");
        }

        private void EnsureNotDisposed()
        {
            if (isDisposed)
                throw new ObjectDisposedException("RemoteLocker is already disposed");
        }

        private async Task<(IRemoteAsyncLock Locker, string RivalThreadId)> DoTryAcquireLock(string lockId, string threadId)
        {
            if (remoteLocksById.TryGetValue(lockId, out var rival))
            {
                return (null, rival.ThreadId);
            }
            var attempt = 1;
            while (true)
            {
                LockAttemptResult lockAttempt;
                using (metrics.CassandraImplTryLockOp.NewContext(t => { }, FormatLockOperationId(lockId, threadId)))
                    lockAttempt = await remoteAsyncLockImplementation.TryLock(lockId, threadId).ConfigureAwait(false);
                switch (lockAttempt.Status)
                {
                case LockAttemptStatus.Success:
                    var remoteLockState = new RemoteLockState(lockId, threadId, Timestamp.Now.Add(keepLockAliveInterval));
                    if (!remoteLocksById.TryAdd(lockId, remoteLockState))
                        throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threadId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
                    remoteLocksQueue.Add(remoteLockState);
                    return (new RemoteAsyncLockHandle(lockId, threadId, this), null);
                case LockAttemptStatus.AnotherThreadIsOwner:
                    return (null, lockAttempt.OwnerId);
                case LockAttemptStatus.ConcurrentAttempt:
                    var shortSleep = ThreadLocalRandom.Instance.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
                    logger.Warn("remoteLockImplementation.TryLock() returned LockAttemptStatus.ConcurrentAttempt for lockId: {0}, threadId: {1}. Will sleep for {2} ms", lockId, threadId, shortSleep);
                    await Task.Delay(shortSleep);
                    break;
                default:
                    throw new InvalidOperationException($"Invalid LockAttemptStatus: {lockAttempt.Status}");
                }
            }
        }

        private Task DoReleaseLock(string lockId, string threadId)
        {
            if (!remoteLocksById.TryRemove(lockId, out var remoteLockState) || remoteLockState.ThreadId != threadId)
                throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threadId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
            return Unlock(remoteLockState);
        }

        private async Task Unlock(RemoteLockState remoteLockState)
        {
            await remoteLockState.Locker.WaitAsync();
            try
            {
                remoteLockState.NextKeepAliveMoment = null;
                try
                {
                    using (metrics.CassandraImplUnlockOp.NewContext(t => { }, remoteLockState.ToString()))
                    {
                        var isUnlocked = await remoteAsyncLockImplementation.TryUnlock(remoteLockState.LockId, remoteLockState.ThreadId).ConfigureAwait(false);
                        if (!isUnlocked)
                            logger.Error("Cannot unlock. Possible lock metadata corruption for: {0}", remoteLockState);
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "remoteLockImplementation.Unlock() failed for: {0}", remoteLockState);
                }
            }
            finally
            {
                remoteLockState.Locker.Release();
            }
        }

        private void KeepRemoteLocksAlive()
        {
            try
            {
                while (!remoteLocksQueue.IsCompleted)
                {
                    if (remoteLocksQueue.TryTake(out var remoteLockState, Timeout.Infinite))
                    {
                        void FinalAction(TimeSpan elapsed)
                        {
                            if (elapsed < keepLockAliveInterval + lockOperationWarnThreshold)
                                return;
                            metrics.FreezeEvents.For("KeepLockAlive").Increment();
                            logger.Error("KeepLockAlive() took {0} ms for remote lock: {1}", elapsed.TotalMilliseconds, remoteLockState);
                        }

                        using (metrics.KeepLockAliveOp.NewContext(FinalAction, remoteLockState.ToString()))
                            KeepLockAlive(remoteLockState).GetAwaiter().GetResult();
                    }
                }
            }
            catch (Exception e)
            {
                logger.Fatal(e, "RemoteLocksKeeper thread failed");
            }
        }

        private async Task KeepLockAlive(RemoteLockState remoteLockState)
        {
            TimeSpan? timeToSleep = null;
            await remoteLockState.Locker.WaitAsync().ConfigureAwait(false);
            try
            {
                var nextKeepAliveMoment = remoteLockState.NextKeepAliveMoment;
                if (nextKeepAliveMoment == null)
                    return;
                var utcNow = Timestamp.Now;
                if (utcNow < nextKeepAliveMoment)
                    timeToSleep = nextKeepAliveMoment - utcNow;
            }
            finally
            {
                remoteLockState.Locker.Release();
            }

            if (timeToSleep.HasValue)
                await Task.Delay(timeToSleep.Value).ConfigureAwait(false);

            await remoteLockState.Locker.WaitAsync().ConfigureAwait(false);
            try
            {
                if (remoteLockState.NextKeepAliveMoment == null)
                    return;
                var relocked = await TryRelock(remoteLockState).ConfigureAwait(false);
                if (relocked && !remoteLocksQueue.IsAddingCompleted)
                {
                    remoteLockState.NextKeepAliveMoment = Timestamp.Now.Add(keepLockAliveInterval);
                    remoteLocksQueue.Add(remoteLockState);
                }
            }
            finally
            {
                remoteLockState.Locker.Release();
            }
        }

        private async Task<bool> TryRelock(RemoteLockState remoteLockState)
        {
            var attempt = 1;
            while (true)
            {
                try
                {
                    using (metrics.CassandraImplRelockOp.NewContext(t => { },remoteLockState.ToString()))
                    {
                        var relocked = await remoteAsyncLockImplementation.TryRelock(remoteLockState.LockId, remoteLockState.ThreadId);
                        if (!relocked)
                            logger.Error("Cannot relock. Possible lock metadata corruption for: {0}", remoteLockState);
                        return relocked;
                    }
                }
                catch (Exception e)
                {
                    var shortSleep = ThreadLocalRandom.Instance.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
                    logger.Warn(e, "remoteLockImplementation.Relock() failed for: {0}. Will sleep for {1} ms", remoteLockState, shortSleep);
                    await Task.Delay(shortSleep);
                }
            }
        }

        private volatile bool isDisposed;
        private readonly Thread remoteLocksKeeperThread;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly TimeSpan lockOperationWarnThreshold;
        private readonly IRemoteAsyncLockImplementation remoteAsyncLockImplementation;
        private readonly RemoteAsyncLockerMetrics metrics;
        private readonly ILog logger;
        private readonly ConcurrentDictionary<string, RemoteLockState> remoteLocksById = new ConcurrentDictionary<string, RemoteLockState>();
        private readonly BoundedBlockingQueue<RemoteLockState> remoteLocksQueue = new BoundedBlockingQueue<RemoteLockState>(int.MaxValue);

        private class RemoteLockState
        {
            public RemoteLockState(string lockId, string threadId, [System.Diagnostics.CodeAnalysis.NotNull] Timestamp nextKeepAliveMoment)
            {
                LockId = lockId;
                ThreadId = threadId;
                NextKeepAliveMoment = nextKeepAliveMoment;
            }

            public string LockId { get; }

            public string ThreadId { get; }
            
            public SemaphoreSlim Locker { get; } = new SemaphoreSlim(1, 1);

            [CanBeNull]
            public Timestamp NextKeepAliveMoment { get; set; }

            public override string ToString()
            {
                return $"LockId: {LockId}, ThreadId: {ThreadId}, NextKeepAliveMoment: {NextKeepAliveMoment}";
            }
        }
    }
}