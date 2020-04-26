using System;
using System.Collections.Generic;

using Cassandra;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock.Async.Cluster
{
    public class CassandraCluster : ICassandraCluster
    {
        private readonly ICluster cluster;

        public CassandraCluster(ICluster cluster)
        {
            this.cluster = cluster;
        }

        public ISession RetrieveClusterConnection()
        {
            return cluster.Connect();
        }

        public ISession RetrieveKeyspaceConnection(string keyspaceName)
        {
            return cluster.Connect(keyspaceName);
        }

        public static CassandraCluster CreateFromConnectionString(
            string connectionString,
            [CanBeNull] Action<Builder> configureCluster = null)
        {
            var clusterBuilder = new CassandraConnectionStringBuilder(connectionString)
                .MakeClusterBuilder()
                .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true))
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum))
                .WithCompression(CompressionType.Snappy)
                .WithRetryPolicy(new DefaultRetryPolicy());

            configureCluster?.Invoke(clusterBuilder);
            var cluster = clusterBuilder.Build();
            return new CassandraCluster(cluster);
        }

        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            var session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(keyspaceName, replication, durableWrites);
        }
    }
}