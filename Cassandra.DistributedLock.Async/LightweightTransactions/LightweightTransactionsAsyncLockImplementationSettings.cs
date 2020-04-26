using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async.LightweightTransactions
{
    public class LightweightTransactionsAsyncLockImplementationSettings
    {
        public LightweightTransactionsAsyncLockImplementationSettings(
            [NotNull] ITimestampProvider timestampProvider,
            [NotNull] string keyspaceName,
            [NotNull] string tableName,
            TimeSpan lockTtl,
            TimeSpan lockMetadataTtl,
            TimeSpan keepLockAliveInterval,
            int changeLockRowThreshold)
        {
            TimestampProvider = timestampProvider;
            KeyspaceName = keyspaceName;
            TableName = tableName;
            LockTtl = lockTtl;
            LockMetadataTtl = lockMetadataTtl;
            KeepLockAliveInterval = keepLockAliveInterval;
            if (changeLockRowThreshold <= 0)
                throw new ArgumentException("ChangeRowThreshold must be positive integer", nameof(changeLockRowThreshold));
            ChangeLockRowThreshold = changeLockRowThreshold;
        }

        [NotNull]
        public ITimestampProvider TimestampProvider { get; }

        [NotNull]
        public string KeyspaceName { get; }

        [NotNull]
        public string TableName { get; }

        public TimeSpan LockTtl { get; }
        public TimeSpan LockMetadataTtl { get; }
        public TimeSpan KeepLockAliveInterval { get; }
        public int ChangeLockRowThreshold { get; }

        [NotNull]
        public static LightweightTransactionsAsyncLockImplementationSettings Default([NotNull] string keyspaceName, [NotNull] string columnFamilyName)
        {
            return new LightweightTransactionsAsyncLockImplementationSettings(new DefaultTimestampProvider(), keyspaceName, columnFamilyName, TimeSpan.FromMinutes(3), TimeSpan.FromDays(7), TimeSpan.FromSeconds(10), 1000);
        }
    }
}