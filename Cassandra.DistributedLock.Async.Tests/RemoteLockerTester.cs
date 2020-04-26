using System;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.DistributedLock.Async.Tests.FailedCassandra;
using Cassandra.DistributedLock.Async.Tests.Logging;

using SkbKontur.Cassandra.DistributedLock.Async;
using SkbKontur.Cassandra.DistributedLock.Async.Cluster;
using SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker;
using SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit;

using Vostok.Logging.Abstractions;
using Vostok.Metrics;

namespace Cassandra.DistributedLock.Async.Tests
{
    public class RemoteLockerTester : IDisposable, IRemoteAsyncLockCreator
    {
        public RemoteLockerTester(RemoteLockerTesterConfig config = null)
        {
            config = config ?? RemoteLockerTesterConfig.Default();
            var localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            ICassandraCluster cassandraCluster = CassandraCluster.CreateFromConnectionString(config.CassandraConnectionString);
            if (config.CassandraFailProbability.HasValue)
                cassandraCluster = new FailedCassandraCluster(cassandraCluster, config.CassandraFailProbability.Value);
            var timestampProvider = new StochasticTimestampProvider(config.TimestampProviderStochasticType, config.LockTtl);
            var implementationSettings = new TwoPhaseCommitAsyncLockImplementationSettings(timestampProvider, SingleCassandraNodeSetUpFixture.RemoteLockKeyspace, SingleCassandraNodeSetUpFixture.RemoteLockColumnFamily, config.LockTtl, config.LockMetadataTtl, config.KeepLockAliveInterval, config.ChangeLockRowThreshold);
            var cassandraRemoteLockImplementation = new TwoPhaseCommitAsyncLockImplementation(cassandraCluster, implementationSettings);
            remoteLockers = new RemoteAsyncLocker[config.LockersCount];
            remoteLockerMetrics = new RemoteAsyncLockerMetrics("dummyKeyspace", new DevNullMetricContext());
            if (localRivalOptimizationIsEnabled)
            {
                var remoteLocker = new RemoteAsyncLocker(cassandraRemoteLockImplementation, remoteLockerMetrics, logger);
                for (var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = remoteLocker;
            }
            else
            {
                for (var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = new RemoteAsyncLocker(new TwoPhaseCommitAsyncLockImplementation(cassandraCluster, implementationSettings), remoteLockerMetrics, logger);
            }
            // it is important to use another CassandraCluster (with another setting of attempts, for example)
            cassandraRemoteLockImplementationForChecks = new TwoPhaseCommitAsyncLockImplementation(CassandraCluster.CreateFromConnectionString(SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings()), implementationSettings);
        }

        public void Dispose()
        {
            var disposeTasks = new Task[remoteLockers.Length];
            for (var i = 0; i < remoteLockers.Length; i++)
            {
                var localI = i;
                disposeTasks[i] = Task.Factory.StartNew(() => remoteLockers[localI].DisposeAsync().GetAwaiter().GetResult());
            }
            Task.WaitAll(disposeTasks);
            LogRemoteLockerPerfStat();
        }

        private void LogRemoteLockerPerfStat()
        {
            //TODO: metrics stats
            // var metricsData = remoteLockerMetrics.Context.DataProvider.CurrentMetricsData;
            // var metricsReport = StringReport.RenderMetrics(metricsData, () => new HealthStatus());
            // Console.Out.WriteLine(metricsReport);
        }

        public IRemoteAsyncLockCreator this[int index] => remoteLockers[index];

        public Task<IRemoteAsyncLock> Lock(string lockId)
        {
            return remoteLockers.Single().Lock(lockId);
        }

        public Task<IRemoteAsyncLock> TryGetLock(string lockId)
        {
            return remoteLockers.Single().TryGetLock(lockId);
        }

        public Task<string[]> GetThreadsInMainRow(string lockId)
        {
            return cassandraRemoteLockImplementationForChecks.GetLockThreads(lockId);
        }

        public Task<string[]> GetThreadsInShadeRow(string lockId)
        {
            return cassandraRemoteLockImplementationForChecks.GetShadeThreads(lockId);
        }

        public Task<LockMetadata> GetLockMetadata(string lockId)
        {
            return cassandraRemoteLockImplementationForChecks.GetLockMetadata(lockId);
        }

        private readonly RemoteAsyncLocker[] remoteLockers;
        private readonly RemoteAsyncLockerMetrics remoteLockerMetrics;
        private readonly TwoPhaseCommitAsyncLockImplementation cassandraRemoteLockImplementationForChecks;
        private static readonly ILog logger = Log4NetConfiguration.RootLogger.ForContext(nameof(RemoteLockerTester));
    }
}