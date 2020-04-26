using System;
using System.IO;
using System.Linq;

using Cassandra.DistributedLock.Async.Tests.Logging;

using NUnit.Framework;

using SkbKontur.Cassandra.DistributedLock.Async.Cluster;
using SkbKontur.Cassandra.Local;

namespace Cassandra.DistributedLock.Async.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [OneTimeSetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            node.Restart(timeout : TimeSpan.FromMinutes(1));

            cassandraCluster = CassandraCluster.CreateFromConnectionString(CreateCassandraClusterSettings());
            cassandraCluster.CreateKeyspaceIfNotExists(RemoteLockKeyspace);
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            node.Stop();
        }

        public static string CreateCassandraClusterSettings()
        {
            var connectionBuilder = new CassandraConnectionStringBuilder
                {
                    ClusterName = node.ClusterName,
                    ContactPoints = node.SeedAddresses,
                    Port = node.CqlPort
                };
            return connectionBuilder.ToString();
        }

        public static void TruncateAllTables()
        {
            var session = cassandraCluster.RetrieveClusterConnection();
            var tablesToTruncate = session.Cluster.Metadata
                .GetTables(RemoteLockKeyspace)
                .Where(c => c.StartsWith(RemoteLockColumnFamily, StringComparison.OrdinalIgnoreCase));
            foreach (var table in tablesToTruncate)
            {
                session.Execute($"TRUNCATE TABLE \"{RemoteLockKeyspace}\".\"{table}\"", ConsistencyLevel.All);
            }
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        public const string RemoteLockKeyspace = "TestRemoteLockKeyspace";
        public const string RemoteLockColumnFamily = "TestRemoteLockCf";

        private const string cassandraTemplates = @"cassandra-local\cassandra";

        private static LocalCassandraNode node;
        private static CassandraCluster cassandraCluster;
    }
}