using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using Cassandra.DataStax.Graph;
using Cassandra.Metrics;

using SkbKontur.Cassandra.TimeBasedUuid;

#pragma warning disable 618

namespace Cassandra.DistributedLock.Async.Tests.FailedCassandra
{
    public class FailedCassandraSession : ISession
    {
        public FailedCassandraSession(ISession session, double failProbability)
        {
            this.session = session;
            this.failProbability = failProbability;
        }

        private void MayBeFail()
        {
            if (ThreadLocalRandom.Instance.NextDouble() < failProbability)
                throw new FailedCassandraClusterException("Intentional error from cassandra");
        }

        private readonly ISession session;
        private readonly double failProbability;
        
        public void Dispose()
        {
            session.Dispose();
        }

        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            MayBeFail();
            return session.BeginExecute(statement, callback, state);
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            MayBeFail();
            return session.BeginExecute(cqlQuery, consistency, callback, state);
        }

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            MayBeFail();
            return session.BeginPrepare(cqlQuery, callback, state);
        }

        public void ChangeKeyspace(string keyspaceName)
        {
            session.ChangeKeyspace(keyspaceName);
        }

        public void CreateKeyspace(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            session.CreateKeyspace(keyspaceName, replication, durableWrites);
        }

        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            session.CreateKeyspaceIfNotExists(keyspaceName, replication, durableWrites);
        }

        public void DeleteKeyspace(string keyspaceName)
        {
            session.DeleteKeyspace(keyspaceName);
        }

        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            session.DeleteKeyspaceIfExists(keyspaceName);
        }

        public RowSet EndExecute(IAsyncResult ar)
        {
            MayBeFail();
            return session.EndExecute(ar);
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            MayBeFail();
            return session.EndPrepare(ar);
        }

        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            MayBeFail();
            return session.Execute(statement, executionProfileName);
        }

        public RowSet Execute(IStatement statement)
        {
            MayBeFail();
            return session.Execute(statement);
        }

        public RowSet Execute(string cqlQuery)
        {
            MayBeFail();
            return session.Execute(cqlQuery);
        }

        public RowSet Execute(string cqlQuery, string executionProfileName)
        {
            MayBeFail();
            return session.Execute(cqlQuery, executionProfileName);
        }

        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            MayBeFail();
            return session.Execute(cqlQuery, consistency);
        }

        public RowSet Execute(string cqlQuery, int pageSize)
        {
            MayBeFail();
            return session.Execute(cqlQuery, pageSize);
        }

        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            MayBeFail();
            return session.ExecuteAsync(statement);
        }

        public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)
        {
            MayBeFail();
            return session.ExecuteAsync(statement, executionProfileName);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            MayBeFail();
            return session.Prepare(cqlQuery);
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            MayBeFail();
            return session.Prepare(cqlQuery, customPayload);
        }

        public PreparedStatement Prepare(string cqlQuery, string keyspace)
        {
            MayBeFail();
            return session.Prepare(cqlQuery, keyspace);
        }

        public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            MayBeFail();
            return session.Prepare(cqlQuery, keyspace, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery)
        {
            MayBeFail();
            return session.PrepareAsync(cqlQuery);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            MayBeFail();
            return session.PrepareAsync(cqlQuery, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace)
        {
            MayBeFail();
            return session.PrepareAsync(cqlQuery, keyspace);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            MayBeFail();
            return session.PrepareAsync(cqlQuery, keyspace, customPayload);
        }

        public IDriverMetrics GetMetrics()
        {
            MayBeFail();
            return session.GetMetrics();
        }

        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            MayBeFail();
            return session.ExecuteGraph(statement);
        }

        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement)
        {
            MayBeFail();
            return session.ExecuteGraphAsync(statement);
        }

        public GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName)
        {
            MayBeFail();
            return session.ExecuteGraph(statement, executionProfileName);
        }

        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement statement, string executionProfileName)
        {
            MayBeFail();
            return session.ExecuteGraphAsync(statement, executionProfileName);
        }

        public Task ShutdownAsync()
        {
            return session.ShutdownAsync();
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            session.WaitForSchemaAgreement(rs);
        }

        public bool WaitForSchemaAgreement(IPEndPoint forHost)
        {
            return session.WaitForSchemaAgreement(forHost);
        }

        public int BinaryProtocolVersion => session.BinaryProtocolVersion;
        public ICluster Cluster => session.Cluster;
        public bool IsDisposed => session.IsDisposed;
        public string Keyspace => session.Keyspace;
        public UdtMappingDefinitions UserDefinedTypes => session.UserDefinedTypes;
        public string SessionName => session.SessionName;
    }
}