namespace SkbKontur.Cassandra.DistributedLock.Async.TwoPhaseCommit
{
    public enum LockStatus
    {
        Sleeping = 1,
        Shadow = 2,
        Main = 3,
    }
}