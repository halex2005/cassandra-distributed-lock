using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.DistributedLock.Async.Tests.Logging;

using NUnit.Framework;

using SkbKontur.Cassandra.DistributedLock.Async;
using SkbKontur.Cassandra.DistributedLock.Async.Cluster;
using SkbKontur.Cassandra.DistributedLock.Async.LightweightTransactions;
using SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker;
using SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit;

using Vostok.Logging.Abstractions;
using Vostok.Metrics;

namespace Cassandra.DistributedLock.Async.Tests
{
    [TestFixture(LockImplementationToTest.LightweightTransactions)]
    [TestFixture(LockImplementationToTest.TwoPhaseCommit)]
    public class RemoteLockTest
    {
        private readonly LockImplementationToTest lockImpl;

        public RemoteLockTest(LockImplementationToTest lockImpl)
        {
            this.lockImpl = lockImpl;
        }

        [SetUp]
        public void OneTimeSetUp()
        {
            var cassandraCluster = CassandraCluster.CreateFromConnectionString(SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings());
            var settings = new TwoPhaseCommitAsyncLockImplementationSettings(
                new DefaultTimestampProvider(),
                SingleCassandraNodeSetUpFixture.RemoteLockKeyspaceTwoPhaseCommit,
                SingleCassandraNodeSetUpFixture.RemoteLockKeyspace, 
                TimeSpan.FromMinutes(3),
                TimeSpan.FromDays(30),
                TimeSpan.FromSeconds(5),
                10);
            var lwtSettings = new LightweightTransactionsAsyncLockImplementationSettings(
                new DefaultTimestampProvider(),
                SingleCassandraNodeSetUpFixture.RemoteLockKeyspaceLightweightTransactions,
                SingleCassandraNodeSetUpFixture.RemoteLockColumnFamily, 
                TimeSpan.FromMinutes(3),
                TimeSpan.FromDays(30),
                TimeSpan.FromSeconds(5),
                10);
            remoteLockImplementation = lockImpl switch
               {
                   LockImplementationToTest.LightweightTransactions => new LightweightTransactionsAsyncLockImplementation(cassandraCluster, lwtSettings),
                   LockImplementationToTest.TwoPhaseCommit => new TwoPhaseCommitAsyncLockImplementation(cassandraCluster, settings),
                   _ => throw new InvalidOperationException("")
               };
        }

        [SetUp]
        public void SetUp()
        {
            logger.Info("Start SetUp, runningThreads = {0}", runningThreads);
            runningThreads = 0;
            isEnd = false;
            threads = new List<Thread>();
        }

        [TearDown]
        public void TearDown()
        {
            logger.Info("Start TearDown, runningThreads = {0}", runningThreads);
            foreach (var thread in threads ?? new List<Thread>())
            {
                if (thread.IsAlive)
                    thread.Abort();
            }
        }

        [TestCase(LocalRivalOptimization.Disabled, Category = "LongRunning")]
        [TestCase(LocalRivalOptimization.Enabled, Category = "LongRunning")]
        public void StressTest(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(30, TimeSpan.FromSeconds(60), localRivalOptimization);
        }

        [TestCase(LocalRivalOptimization.Disabled)]
        [TestCase(LocalRivalOptimization.Enabled)]
        public void TestIncrementDecrementLock(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(10, TimeSpan.FromSeconds(10), localRivalOptimization);
        }

        private void DoTestIncrementDecrementLock(int threadCount, TimeSpan runningTimeInterval, LocalRivalOptimization localRivalOptimization)
        {
            var remoteLockCreators = PrepareRemoteLockCreators(threadCount, localRivalOptimization, remoteLockImplementation);

            for (var i = 0; i < threadCount; i++)
                AddThread(IncrementDecrementAction, remoteLockCreators[i]);
            RunThreads(runningTimeInterval);
            JoinThreads();

            DisposeRemoteLockCreators(remoteLockCreators).GetAwaiter().GetResult();
        }

        private void IncrementDecrementAction(IRemoteAsyncLockCreator lockCreator)
        {
            try
            {
                var remoteLock = lockCreator.Lock(lockId).GetAwaiter().GetResult();
                try
                {
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    Thread.Sleep(1000);
                    CheckLocks(remoteLock.ThreadId).GetAwaiter().GetResult();
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
                finally
                {
                    remoteLock.DisposeAsync().GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private int ReadX()
        {
            return Interlocked.CompareExchange(ref x, 0, 0);
        }

        private async Task CheckLocks(string threadId)
        {
            try
            {
                if (remoteLockImplementation is TwoPhaseCommitAsyncLockImplementation twoPhaseCommitImpl)
                {
                    var locks = await twoPhaseCommitImpl.GetLockThreads(lockId);
                    logger.Info("Locks: " + string.Join(", ", locks));
                    Assert.That(locks.Length <= 1, "Too many locks");
                    Assert.That(locks.Length == 1);
                    Assert.AreEqual(threadId, locks[0]);
                    var lockShades = twoPhaseCommitImpl.GetShadeThreads(lockId);
                    logger.Info("LockShades: " + string.Join(", ", lockShades));
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private void AddThread(Action<IRemoteAsyncLockCreator> shortAction, IRemoteAsyncLockCreator lockCreator)
        {
            var thread = new Thread(() => MakePeriodicAction(shortAction, lockCreator));
            thread.Start();
            threads.Add(thread);
        }

        private void JoinThreads()
        {
            logger.Info("JoinThreads. begin");
            isEnd = true;
            running.Set();
            var timeout = TimeSpan.FromMinutes(5);
            foreach (var thread in threads)
                Assert.That(thread.Join(timeout), $"Thread {thread.ManagedThreadId} didn't finish in {timeout}");
            logger.Info("JoinThreads. end");
        }

        private void RunThreads(TimeSpan runningTimeInterval)
        {
            logger.Info("RunThreads. begin, runningThreads = {0}", runningThreads);
            running.Set();
            Thread.Sleep(runningTimeInterval);
            running.Reset();
            while (Interlocked.CompareExchange(ref runningThreads, 0, 0) != 0)
            {
                Thread.Sleep(50);
                logger.Info("Wait runningThreads = 0. Now runningThreads = {0}", runningThreads);
                foreach (var thread in threads)
                {
                    if (!thread.IsAlive)
                        throw new Exception("Thread is dead");
                }
            }
            logger.Info("RunThreads. end");
        }

        private void MakePeriodicAction(Action<IRemoteAsyncLockCreator> shortAction, IRemoteAsyncLockCreator lockCreator)
        {
            try
            {
                while (!isEnd)
                {
                    running.WaitOne();
                    Interlocked.Increment(ref runningThreads);
                    shortAction(lockCreator);
                    Interlocked.Decrement(ref runningThreads);
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static IRemoteAsyncLockCreator[] PrepareRemoteLockCreators(int threadCount, LocalRivalOptimization localRivalOptimization, IRemoteAsyncLockImplementation remoteLockImplementation)
        {
            var remoteLockCreators = new IRemoteAsyncLockCreator[threadCount];
            var remoteLockerMetrics = new RemoteAsyncLockerMetrics("unknown", new DevNullMetricContext());
            if (localRivalOptimization == LocalRivalOptimization.Enabled)
            {
                var singleRemoteLocker = new RemoteAsyncLocker(remoteLockImplementation, remoteLockerMetrics, logger);
                for (var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = singleRemoteLocker;
            }
            else
            {
                for (var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = new RemoteAsyncLocker(remoteLockImplementation, remoteLockerMetrics, logger);
            }
            return remoteLockCreators;
        }

        private static Task DisposeRemoteLockCreators(IRemoteAsyncLockCreator[] remoteLockCreators)
        {
            return Task.WhenAll(
                remoteLockCreators.Select(
                    x => ((RemoteAsyncLocker)x).DisposeAsync().AsTask()));
        }

        private const string lockId = "IncDecLock";
        private int x;
        private IRemoteAsyncLockImplementation remoteLockImplementation;
        private volatile bool isEnd;
        private int runningThreads;
        private List<Thread> threads;
        private readonly ManualResetEvent running = new ManualResetEvent(false);
        private static readonly ILog logger = Log4NetConfiguration.RootLogger.ForContext(nameof(RemoteLockTest));
    }
}