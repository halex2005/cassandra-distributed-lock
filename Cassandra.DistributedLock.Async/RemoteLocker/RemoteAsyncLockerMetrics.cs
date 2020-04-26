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
            LockOp = Context.CreateSummary("Lock", "operationId");
            TryGetLockOp = Context.CreateSummary("TryGetLock", "operationId");
            TryAcquireLockOp = Context.CreateSummary("TryAcquireLock", "operationId");
            ReleaseLockOp = Context.CreateSummary("ReleaseLock", "operationId");
            KeepLockAliveOp = Context.CreateSummary("KeepLock", "operationId");
            CassandraImplTryLockOp = Context.CreateSummary("CassandraImpl.TryLock", "operationId");
            CassandraImplRelockOp = Context.CreateSummary("CassandraImpl.Relock", "operationId");
            CassandraImplUnlockOp = Context.CreateSummary("CassandraImpl.Unlock", "operationId");

            var eventsConfig = new IntegerGaugeConfig { Unit = "events/hour"};
            FreezeEvents = Context.CreateIntegerGauge("FreezeEvents","method", eventsConfig);
        }

        public IMetricContext Context { get; }
        public IMetricGroup1<ITimer> LockOp { get; }
        public IMetricGroup1<ITimer> TryGetLockOp { get; }
        public IMetricGroup1<ITimer> TryAcquireLockOp { get; }
        public IMetricGroup1<ITimer> ReleaseLockOp { get; }
        public IMetricGroup1<ITimer> KeepLockAliveOp { get; }
        public IMetricGroup1<ITimer> CassandraImplTryLockOp { get; }
        public IMetricGroup1<ITimer> CassandraImplRelockOp { get; }
        public IMetricGroup1<ITimer> CassandraImplUnlockOp { get; }
        public IMetricGroup1<IIntegerGauge> FreezeEvents { get; }
    }
}