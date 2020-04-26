namespace Cassandra.DistributedLock.Async.Tests
{
    public enum TimestampProviderStochasticType
    {
        None,
        OnlyPositive,
        BothPositiveAndNegative
    }
}