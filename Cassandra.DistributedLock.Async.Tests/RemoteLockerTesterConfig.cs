using System;

namespace Cassandra.DistributedLock.Async.Tests
{
    public class RemoteLockerTesterConfig
    {
        public int LockersCount { get; set; }
        public LocalRivalOptimization LocalRivalOptimization { get; set; }
        public TimeSpan LockTtl { get; set; }
        public TimeSpan LockMetadataTtl { get; set; }
        public TimeSpan KeepLockAliveInterval { get; set; }
        public int ChangeLockRowThreshold { get; set; }
        public TimestampProviderStochasticType TimestampProviderStochasticType { get; set; }
        public string CassandraConnectionString { get; set; }
        public double? CassandraFailProbability { get; set; }
        public TimeSpan Timeout { get; set; }
        public int Attempts { get; set; }

        public static RemoteLockerTesterConfig Default()
        {
            return new RemoteLockerTesterConfig
                {
                    LockersCount = 1,
                    LocalRivalOptimization = LocalRivalOptimization.Enabled,
                    LockTtl = TimeSpan.FromSeconds(10),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(2),
                    ChangeLockRowThreshold = 10,
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraConnectionString = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(),
                    CassandraFailProbability = null,
                    Attempts = 5,
                    Timeout = TimeSpan.FromSeconds(6)
                };
        }
    }
}