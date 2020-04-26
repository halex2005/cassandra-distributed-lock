using System;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using JetBrains.Annotations;

using SkbKontur.Cassandra.DistributedLock.Async.Cluster;

namespace SkbKontur.Cassandra.DistributedLock.Async.LightweightTransactions
{
    internal class LightweightTransactionsAsyncLockOperationsPerformer
    {
        public LightweightTransactionsAsyncLockOperationsPerformer(
            ICassandraCluster cassandraCluster,
            LightweightTransactionsAsyncLockImplementationSettings settings)
        {
            cassandraClient = new CassandraClient(
                cassandraCluster.RetrieveKeyspaceConnection(settings.KeyspaceName),
                new LockEntityColumnMappings(settings.TableName));
            timestampProvider = settings.TimestampProvider;
        }

        public async Task<(bool applied, LockMetadata existingLock)> TryWriteThread([NotNull] string lockRowId, [NotNull] string threadId, TimeSpan ttl)
        {
            var timestamp = new DateTimeOffset(GetNowTicks(), TimeSpan.Zero);
            var applied = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .Insert(new LockRowEntity
                            {
                                LockId = lockRowId,
                                ThreadId = threadId,
                                LastWriteTime = timestamp,
                                Payload = new byte[] {0}
                            })
                        .IfNotExists()
                        .SetTTL((int)ttl.TotalSeconds)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            if (applied.Applied)
            {
                return (true, new LockMetadata(lockRowId, 1, threadId, timestamp.UtcTicks));
            }

            return (false, new LockMetadata(applied.Existing.LockId, 1, applied.Existing.ThreadId, applied.Existing.LastWriteTime.UtcTicks));
        }

        public async Task<bool> TryRenewThread([NotNull] string lockRowId, [NotNull] string threadId, TimeSpan ttl)
        {
            var entity = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .FirstOrDefault(x => x.LockId == lockRowId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            if (entity == null || entity.ThreadId != threadId)
            {
                return false;
            }

            var timestamp = new DateTimeOffset(GetNowTicks(), TimeSpan.Zero);
            await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .Insert(new LockRowEntity
                            {
                                LockId = lockRowId,
                                ThreadId = threadId,
                                LastWriteTime = timestamp,
                                Payload = new byte[] {0}
                            })
                        .SetTimestamp(timestamp)
                        .SetTTL((int)ttl.TotalSeconds)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            return true;
        }

        public async Task<bool> DeleteThread([NotNull] string lockRowId, [NotNull] string threadId)
        {
            var appliedInfo = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .Where(x => x.LockId == lockRowId)
                        .DeleteIf(x => x.ThreadId == threadId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            return appliedInfo.Applied;
        }

        [ItemCanBeNull]
        public async Task<string> GetLockAliveThread([NotNull] string lockRowId)
        {
            var lockThread = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .FirstOrDefault(x => x.LockId == lockRowId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);
        
            return lockThread?.ThreadId;
        }

        [ItemCanBeNull]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("ReSharper", "PossibleInvalidOperationException")]
        public async Task<LockMetadata> TryGetLockMetadata([NotNull] string lockId)
        {
            var metadataRow = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .FirstOrDefault(x => x.LockId == lockId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            if (metadataRow == null)
            {
                return null;
            }

            return new LockMetadata(lockId, 1, metadataRow.ThreadId, metadataRow.LastWriteTime.UtcTicks);
        }

        private long GetNowTicks()
        {
            var ticks = timestampProvider.GetNowTicks();
            while (true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + ticksPerMicrosecond);
                if (Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        private class LockEntityColumnMappings : Mappings
        {
            public LockEntityColumnMappings(string tableName)
            {
                For<LockRowEntity>()
                    .ExplicitColumns()
                    .Column(x => x.LockId, c => c.WithName("lock_id"))
                    .Column(x => x.ThreadId, c => c.WithName("thread_id"))
                    .Column(x => x.Payload, c => c.WithName("payload"))
                    .Column(x => x.LastWriteTime, c => c.WithName("last_write_time"))
                    .PartitionKey(x => x.LockId)
                    .TableName(tableName);
            }
        }

        private class LockRowEntity
        {
            public string LockId { get; set; }
            public string ThreadId { get; set; }
            public byte[] Payload { get; set; }
            public DateTimeOffset LastWriteTime { get; set; }
        }

        private const long ticksPerMicrosecond = 10;
        private readonly CassandraClient cassandraClient;
        private readonly ITimestampProvider timestampProvider;
        private long lastTicks;
    }
}