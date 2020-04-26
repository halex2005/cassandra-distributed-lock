namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public interface ITimestampProvider
    {
        long GetNowTicks();
    }
}