using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using JetBrains.Annotations;

using SkbKontur.Cassandra.DistributedLock.Async.Cluster;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit
{
    internal class TwoPhaseCommitAsyncLockOperationsPerformer
    {
        private readonly ILog logger;

        public TwoPhaseCommitAsyncLockOperationsPerformer(
            ICassandraCluster cassandraCluster,
            TwoPhaseCommitAsyncLockImplementationSettings settings,
            ILog logger)
        {
            this.logger = logger.ForContext<TwoPhaseCommitAsyncLockOperationsPerformer>();
            cassandraClient = new CassandraClient(
                cassandraCluster.RetrieveKeyspaceConnection(settings.KeyspaceName),
                new LockEntityColumnMappings(settings.TableName));
            timestampProvider = settings.TimestampProvider;
            lockMetadataTtl = settings.LockMetadataTtl;
        }

        public Task WriteThread([NotNull] string lockRowId, [NotNull] string threadId, LockStatus lockStatus, TimeSpan ttl)
        {
            if (lockStatus == LockStatus.Main)
            {
                return WriteMainThreadAndMetadata(lockRowId, threadId, lockStatus, ttl);
            }
            logger.Info($"Write thread {lockStatus} (Lock={lockRowId}, Thread={threadId})");
            return cassandraClient.MakeInConnectionAsync(
                (Table<LockRowEntity> table) => table
                    .Insert(new LockRowEntity
                        {
                            LockId = lockRowId,
                            ThreadId = threadId,
                            LockStatus = (sbyte)lockStatus,
                            Payload = new byte[] {0}
                        })
                    .SetTTL((int)ttl.TotalSeconds)
                    .SetTimestamp(new DateTimeOffset(GetNowTicks(), TimeSpan.Zero))
                    .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .ExecuteAsync());
        }

        private async Task WriteMainThreadAndMetadata(string lockRowId, string threadId, LockStatus lockStatus, TimeSpan ttl)
        {
            var timestamp = new DateTimeOffset(GetNowTicks(), TimeSpan.Zero);
            var entityTable = await cassandraClient.GetTableAsync<LockRowEntity>().ConfigureAwait(false);
            var entityMetadataTable = await cassandraClient.GetTableAsync<LockMetadataEntity>().ConfigureAwait(false);

            logger.Info($"Write lock and metadata (Lock={lockRowId}, Thread={threadId}");

            var session = cassandraClient.Session.CreateBatch(BatchType.Unlogged);

            session.Append(entityTable
                .Insert(new LockRowEntity
                    {
                        LockId = lockRowId,
                        ThreadId = threadId,
                        LockStatus = (sbyte)lockStatus,
                        Payload = new byte[] {0}
                    })
                .SetTTL((int)ttl.TotalSeconds)
                .SetTimestamp(timestamp)
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum));

            session.Append(entityMetadataTable
                .Insert(new LockMetadataEntity
                    {
                        LockId = lockRowId,
                        ProbableOwnerThreadId = threadId,
                        LastWriteTimestamp = timestamp
                    })
                .SetTTL((int)lockMetadataTtl.TotalSeconds)
                .SetTimestamp(timestamp)
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum));

            await session.ExecuteAsync();
        }

        public Task DeleteThread([NotNull] string lockRowId, [NotNull] string threadId)
        {
            logger.Info($"Delete thread (Lock={lockRowId}, Thread={threadId})");
            return cassandraClient.MakeInConnectionAsync(
                (Table<LockRowEntity> table) => table
                    .Where(x => x.LockId == lockRowId && x.ThreadId == threadId)
                    .Delete()
                    .SetTimestamp(new DateTimeOffset(GetNowTicks(), TimeSpan.Zero))
                    .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .ExecuteAsync());
        }

        public async Task<bool> ThreadAlive([NotNull] string lockRowId, [NotNull] string threadId)
        {
            var lockThread = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .FirstOrDefault(x => x.LockId == lockRowId && x.ThreadId == threadId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            return lockThread != null && lockThread.LockStatus == (sbyte)LockStatus.Main;
        }

        [ItemNotNull]
        public async Task<string[]> SearchThreads([NotNull] string lockRowId, LockStatus statusToSearch)
        {
            var rows = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockRowEntity> table) => table
                        .Where(x => x.LockId == lockRowId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            return rows
                .Where(x =>
                    x.Payload != null
                    && x.Payload.Length != 0
                    && x.LockStatus == (sbyte)statusToSearch)
                .Select(x => x.ThreadId)
                .Distinct()
                .ToArray();
        }

        [ItemCanBeNull]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("ReSharper", "PossibleInvalidOperationException")]
        public async Task<LockMetadata> TryGetLockMetadata([NotNull] string lockId)
        {
            var metadataRow = await cassandraClient
                .MakeInConnectionAsync(
                    (Table<LockMetadataEntity> table) => table
                        .FirstOrDefault(x => x.LockId == lockId)
                        .SetConsistencyLevel(ConsistencyLevel.LocalQuorum)
                        .ExecuteAsync())
                .ConfigureAwait(false);

            if (metadataRow == null)
            {
                return null;
            }

            return new LockMetadata(lockId, 1, metadataRow.ProbableOwnerThreadId, metadataRow.LastWriteTimestamp.UtcTicks);
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
                    .Column(x => x.Payload)
                    .Column(x => x.LockStatus, c => c.WithName("lock_status"))
                    .PartitionKey(x => x.LockId)
                    .ClusteringKey(x => x.ThreadId)
                    .TableName(tableName);

                For<LockMetadataEntity>()
                    .ExplicitColumns()
                    .Column(x => x.LockId, c => c.WithName("lock_id"))
                    .Column(x => x.ProbableOwnerThreadId, c => c.WithName("probable_owner_thread_id"))
                    .Column(x => x.LastWriteTimestamp, c => c.WithName("last_write_time"))
                    .PartitionKey(x => x.LockId)
                    .TableName(tableName + "_metadata");
            }
        }

        private class LockRowEntity
        {
            public string LockId { get; set; }
            public string ThreadId { get; set; }
            public byte[] Payload { get; set; }
            public sbyte LockStatus { get; set; }
        }

        private class LockMetadataEntity
        {
            public string LockId { get; set; }
            public string ProbableOwnerThreadId { get; set; }
            public DateTimeOffset LastWriteTimestamp { get; set; }
        }

        private const long ticksPerMicrosecond = 10;
        private readonly CassandraClient cassandraClient;
        private readonly ITimestampProvider timestampProvider;
        private readonly TimeSpan lockMetadataTtl;
        private long lastTicks;
    }
}