using System;
using System.Threading.Tasks;

using NUnit.Framework;

namespace Cassandra.DistributedLock.Async.Tests
{
    public class BasicRemoteLockerTests
    {
        [Test]
        public async Task TryLock_SingleLockId()
        {
            using var tester = new RemoteLockerTester();
            
            var lockId = Guid.NewGuid().ToString();
            await using (var lock1 = await tester.TryGetLock(lockId))
            {
                Assert.That(lock1, Is.Not.Null);
                var lock2 = await tester.TryGetLock(lockId);
                Assert.That(lock2, Is.Null);
            }
                
            await using (var lock2 = await tester.TryGetLock(lockId))
            {
                Assert.That(lock2, Is.Not.Null);
            }
        }

        [Test]
        public async Task TryLock_DifferentLockIds()
        {
            using var tester = new RemoteLockerTester();

            var lockId1 = Guid.NewGuid().ToString();
            var lockId2 = Guid.NewGuid().ToString();
            var lockId3 = Guid.NewGuid().ToString();
            await using var lock1 = await tester.TryGetLock(lockId1);
            await using var lock2 = await tester.TryGetLock(lockId2);
            await using var lock3 = await tester.TryGetLock(lockId3);
            Assert.That(lock1, Is.Not.Null);
            Assert.That(lock2, Is.Not.Null);
            Assert.That(lock3, Is.Not.Null);
        }

        [Test]
        public async Task Lock()
        {
            using var tester = new RemoteLockerTester();
            var lockId = Guid.NewGuid().ToString();

            var lock1 = await tester.Lock(lockId);
            Assert.That(lock1, Is.Not.Null);
            var lock2 = await tester.TryGetLock(lockId);
            Assert.That(lock2, Is.Null);
            await lock1.DisposeAsync();

            lock2 = await tester.TryGetLock(lockId);
            Assert.That(lock2, Is.Not.Null);
            await lock2.DisposeAsync();
        }

        [Test]
        public async Task LockIsKeptAlive_Success()
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockersCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(10),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(5),
                    ChangeLockRowThreshold = 10,
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraConnectionString = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(),
                    Attempts = 1,
                    Timeout = TimeSpan.FromSeconds(1)
                };
            using (var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = await tester[0].Lock(lockId);
                await Task.Delay(TimeSpan.FromSeconds(12)); // waiting in total: 12 = 1*1 + 10 + 1 sec
                var lock2 = await tester[1].TryGetLock(lockId);
                Assert.That(lock2, Is.Null);
                await lock1.DisposeAsync();
                
                lock2 = await tester[1].TryGetLock(lockId);
                Assert.That(lock2, Is.Not.Null);
                await lock2.DisposeAsync();
            }
        }

        [Test]
        public async Task LockIsKeptAlive_Failure()
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockersCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(5),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(10),
                    ChangeLockRowThreshold = 10,
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraConnectionString = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(),
                    Attempts = 1,
                    Timeout = TimeSpan.FromSeconds(1)
                };
            using (var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = await tester[0].Lock(lockId);
                await Task.Delay(TimeSpan.FromSeconds(3));
                var lock2 = await tester[1].TryGetLock(lockId);
                Assert.That(lock2, Is.Null);
                await Task.Delay(TimeSpan.FromSeconds(4)); // waiting in total: 3 + 4 = 1*1 + 5 + 1 sec
                lock2 = await tester[1].TryGetLock(lockId);
                Assert.That(lock2, Is.Not.Null);
                await lock2.DisposeAsync();
                await lock1.DisposeAsync();
            }
        }
    }
}