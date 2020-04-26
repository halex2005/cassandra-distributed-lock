namespace Cassandra.DistributedLock.Async.Tests
{
    public enum LockImplementationToTest
    {
        TwoPhaseCommit,
        LightweightTransactions
    }
}