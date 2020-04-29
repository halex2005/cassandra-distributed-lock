using Vostok.Metrics;
using Vostok.Metrics.Grouping;
using Vostok.Metrics.Primitives.Gauge;
using Vostok.Metrics.Primitives.Timer;

namespace SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker
{
    public class RemoteAsyncLockerMetrics
    {
        public RemoteAsyncLockerMetrics(string keyspaceName, IMetricContext metricContext)
        {
            Context = metricContext.WithTag("class", "RemoteAsyncLocker");
            if (!string.IsNullOrEmpty(keyspaceName))
                Context = Context.WithTag("keyspace", keyspaceName);
            LockOp = Context.CreateSummary("Lock");
            TryGetLockOp = Context.CreateSummary("TryGetLock");
            TryAcquireLockOp = Context.CreateSummary("TryAcquireLock");
            ReleaseLockOp = Context.CreateSummary("ReleaseLock");
            KeepLockAliveOp = Context.CreateSummary("KeepLock");
            CassandraImplTryLockOp = Context.CreateSummary("CassandraImpl.TryLock");
            CassandraImplRelockOp = Context.CreateSummary("CassandraImpl.Relock");
            CassandraImplUnlockOp = Context.CreateSummary("CassandraImpl.Unlock");

            var eventsConfig = new IntegerGaugeConfig { Unit = "events/hour"};
            FreezeEvents = Context.CreateIntegerGauge("FreezeEvents","method", eventsConfig);
        }

        public IMetricContext Context { get; }
        public ITimer LockOp { get; }
        public ITimer TryGetLockOp { get; }
        public ITimer TryAcquireLockOp { get; }
        public ITimer ReleaseLockOp { get; }
        public ITimer KeepLockAliveOp { get; }
        public ITimer CassandraImplTryLockOp { get; }
        public ITimer CassandraImplRelockOp { get; }
        public ITimer CassandraImplUnlockOp { get; }
        public IMetricGroup1<IIntegerGauge> FreezeEvents { get; }
    }
}