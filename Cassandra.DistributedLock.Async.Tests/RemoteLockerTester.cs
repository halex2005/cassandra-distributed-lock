using System;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.DistributedLock.Async.Tests.FailedCassandra;
using Cassandra.DistributedLock.Async.Tests.Logging;

using JetBrains.Annotations;

using SkbKontur.Cassandra.DistributedLock.Async;
using SkbKontur.Cassandra.DistributedLock.Async.Cluster;
using SkbKontur.Cassandra.DistributedLock.Async.LightweightTransactions;
using SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker;
using SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit;

using Vostok.Logging.Abstractions;
using Vostok.Metrics;

namespace Cassandra.DistributedLock.Async.Tests
{
    public class RemoteLockerTester : IDisposable, IRemoteAsyncLockCreator
    {
        public RemoteLockerTester(LockImplementationToTest lockImpl, RemoteLockerTesterConfig config = null)
        {
            config = config ?? RemoteLockerTesterConfig.Default();
            var localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            ICassandraCluster cassandraCluster = CassandraCluster.CreateFromConnectionString(config.CassandraConnectionString);
            if (config.CassandraFailProbability.HasValue)
                cassandraCluster = new FailedCassandraCluster(cassandraCluster, config.CassandraFailProbability.Value);
            var timestampProvider = new StochasticTimestampProvider(config.TimestampProviderStochasticType, config.LockTtl);
            
            var implementationSettings = new TwoPhaseCommitAsyncLockImplementationSettings(timestampProvider, SingleCassandraNodeSetUpFixture.RemoteLockKeyspace + "_" + lockImpl, SingleCassandraNodeSetUpFixture.RemoteLockColumnFamily, config.LockTtl, config.LockMetadataTtl, config.KeepLockAliveInterval, config.ChangeLockRowThreshold);
            var lwTransactionsImplementationSettings = new LightweightTransactionsAsyncLockImplementationSettings(timestampProvider, SingleCassandraNodeSetUpFixture.RemoteLockKeyspace + "_" + lockImpl, SingleCassandraNodeSetUpFixture.RemoteLockColumnFamily, config.LockTtl, config.LockMetadataTtl, config.KeepLockAliveInterval, config.ChangeLockRowThreshold);
            IRemoteAsyncLockImplementation cassandraRemoteLockImplementation = lockImpl switch
                {
                    LockImplementationToTest.TwoPhaseCommit => new TwoPhaseCommitAsyncLockImplementation(cassandraCluster, implementationSettings),
                    LockImplementationToTest.LightweightTransactions => new LightweightTransactionsAsyncLockImplementation(cassandraCluster, lwTransactionsImplementationSettings),
                    _ => throw new InvalidOperationException($"unknown lock implementation={lockImpl}")
                };
            
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
                {
                    IRemoteAsyncLockImplementation asyncLockImplementationForChecks = lockImpl switch
                        {
                            LockImplementationToTest.TwoPhaseCommit => new TwoPhaseCommitAsyncLockImplementation(cassandraCluster, implementationSettings),
                            LockImplementationToTest.LightweightTransactions => new LightweightTransactionsAsyncLockImplementation(cassandraCluster, lwTransactionsImplementationSettings),
                            _ => throw new InvalidOperationException($"unknown lock implementation={lockImpl}")
                        };
                    remoteLockers[i] = new RemoteAsyncLocker(asyncLockImplementationForChecks, remoteLockerMetrics, logger);
                }
            }
            // it is important to use another CassandraCluster (with another setting of attempts, for example)
            cassandraRemoteLockImplementationForChecks = lockImpl switch
                {
                    LockImplementationToTest.LightweightTransactions => new LightweightTransactionsAsyncLockImplementation(CassandraCluster.CreateFromConnectionString(SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings()), lwTransactionsImplementationSettings),
                    LockImplementationToTest.TwoPhaseCommit => new TwoPhaseCommitAsyncLockImplementation(CassandraCluster.CreateFromConnectionString(SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings()), implementationSettings),
                    _ => null
                };
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

        public async Task<string[]> GetThreadsInMainRow(string lockId)
        {
            if (cassandraRemoteLockImplementationForChecks is TwoPhaseCommitAsyncLockImplementation twoPhaseCommit)
            {
                return await twoPhaseCommit.GetLockThreads(lockId);
            }
            if (cassandraRemoteLockImplementationForChecks is LightweightTransactionsAsyncLockImplementation lwTransactions)
            {
                var metadata = await lwTransactions.GetLockMetadata(lockId);
                return metadata != null
                    ? new[] { metadata.ProbableOwnerThreadId }
                    : Array.Empty<string>();
            }
            return Array.Empty<string>();
        }

        public Task<string[]> GetThreadsInShadeRow(string lockId)
        {
            if (cassandraRemoteLockImplementationForChecks is TwoPhaseCommitAsyncLockImplementation twoPhaseCommit)
            {
                return twoPhaseCommit?.GetShadeThreads(lockId);
            }
            return Task.FromResult(Array.Empty<string>());
        }

        [ItemCanBeNull]
        public Task<LockMetadata> GetLockMetadata(string lockId)
        {
            if (cassandraRemoteLockImplementationForChecks is TwoPhaseCommitAsyncLockImplementation twoPhaseCommit)
            {
                return twoPhaseCommit.GetLockMetadata(lockId);
            }
            if (cassandraRemoteLockImplementationForChecks is LightweightTransactionsAsyncLockImplementation lwTransactions)
            {
                return lwTransactions.GetLockMetadata(lockId);
            }

            return null;
        }

        private readonly RemoteAsyncLocker[] remoteLockers;
        private readonly RemoteAsyncLockerMetrics remoteLockerMetrics;
        private readonly IRemoteAsyncLockImplementation cassandraRemoteLockImplementationForChecks;
        private static readonly ILog logger = Log4NetConfiguration.RootLogger.ForContext(nameof(RemoteLockerTester));
    }
}