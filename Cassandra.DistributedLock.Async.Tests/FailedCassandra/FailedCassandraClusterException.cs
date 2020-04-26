using System;

namespace Cassandra.DistributedLock.Async.Tests.FailedCassandra
{
    public class FailedCassandraClusterException : Exception
    {
        public FailedCassandraClusterException(string message)
            : base(message)
        {
        }
    }
}