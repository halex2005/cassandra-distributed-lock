using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

namespace SkbKontur.Cassandra.DistributedLock.Async.Cluster
{
    public class CassandraClient
    {
        private readonly ISession session;
        private readonly MappingConfiguration mappingConfiguration;
        private readonly ConcurrentDictionary<Type, TableWrap> tableWraps = new ConcurrentDictionary<Type, TableWrap>();

        public CassandraClient(ISession session, params Mappings[] tableMappings)
        {
            this.session = session;
            mappingConfiguration = new MappingConfiguration().Define(tableMappings);
        }
        
        public Task<TResult> MakeInConnectionAsync<TRowEntity, TResult>(Func<Table<TRowEntity>, Task<TResult>> action)
        {
            if (tableWraps.TryGetValue(typeof(TRowEntity), out var tableWrap) && tableWrap.Table != null)
            {
                var table = tableWrap.GetTableFor<TRowEntity>();
                return action(table);
            }
            return CreateTableAndMakeInConnectionAsync(action);
        }

        private async Task<TResult> CreateTableAndMakeInConnectionAsync<TResult, TRowEntity>(Func<Table<TRowEntity>,Task<TResult>> action)
        {
            var table = await GetTableAsync<TRowEntity>().ConfigureAwait(false);
            return await action(table).ConfigureAwait(false);
        }

        private ValueTask<Table<TRowEntity>> GetTableAsync<TRowEntity>()
        {
            if (tableWraps.TryGetValue(typeof(TRowEntity), out var existingTableWrap) && existingTableWrap.Table != null)
            {
                return new ValueTask<Table<TRowEntity>>(existingTableWrap.GetTableFor<TRowEntity>());
            }

            return CreateTableIfNotExists<TRowEntity>();
        }

        private async ValueTask<Table<TRowEntity>> CreateTableIfNotExists<TRowEntity>()
        {
            var tableWrap = tableWraps.GetOrAdd(typeof(TRowEntity), t => new TableWrap());
            await tableWrap.Locker.WaitAsync();
            try
            {
                if (tableWrap.Table != null)
                {
                    return tableWrap.GetTableFor<TRowEntity>();
                }

                var table = new Table<TRowEntity>(session, mappingConfiguration);
                try
                {
                    table.CreateIfNotExists();
                }
                catch (ServerErrorException ex)
                {
                    if (!ex.Message.Contains("Column family ID mismatch")) throw;
                }
                tableWrap.Table = table;
                return tableWrap.GetTableFor<TRowEntity>();
            }
            finally
            {
                tableWrap.Locker.Release();
            }
        }

        private class TableWrap
        {
            public TableWrap()
            {
                Table = null;
                Locker = new SemaphoreSlim(1, 1);
            }

            public object Table { get; set; }
            
            public SemaphoreSlim Locker { get; }
            
            public Table<T> GetTableFor<T>() => (Table<T>) Table;
        }
    }
}