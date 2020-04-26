namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public enum LockAttemptStatus
    {
        Success,
        AnotherThreadIsOwner,
        ConcurrentAttempt
    }
}