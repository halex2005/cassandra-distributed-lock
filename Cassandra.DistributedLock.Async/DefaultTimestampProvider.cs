using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    public class DefaultTimestampProvider : ITimestampProvider
    {
        public long GetNowTicks()
        {
            return Timestamp.Now.Ticks;
        }
    }
}