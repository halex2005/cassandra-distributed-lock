using Cassandra;

namespace SkbKontur.Cassandra.DistributedLock.Async.Cluster
{
    public interface ICassandraCluster
    {
        ISession RetrieveClusterConnection();
        ISession RetrieveKeyspaceConnection(string keyspaceName);
    }
}