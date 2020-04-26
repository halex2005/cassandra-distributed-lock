using SkbKontur.Cassandra.DistributedLock.Async.Cluster;

namespace Cassandra.DistributedLock.Async.Tests.FailedCassandra
{
    public class FailedCassandraCluster : ICassandraCluster
    {
        public FailedCassandraCluster(ICassandraCluster cassandraCluster, double failProbability)
        {
            this.cassandraCluster = cassandraCluster;
            this.failProbability = failProbability;
        }

        public ISession RetrieveClusterConnection()
        {
            return new FailedCassandraSession(cassandraCluster.RetrieveClusterConnection(), failProbability);
        }

        public ISession RetrieveKeyspaceConnection(string keyspaceName)
        {
            return new FailedCassandraSession(cassandraCluster.RetrieveKeyspaceConnection(keyspaceName), failProbability);
        }

        private readonly double failProbability;
        private readonly ICassandraCluster cassandraCluster;
    }
}