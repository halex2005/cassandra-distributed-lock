using System;

namespace SkbKontur.Cassandra.DistributedLock.Async
{
    internal static class TimeSpanExtensions
    {
        public static TimeSpan Multiply(this TimeSpan value, int factor)
        {
            return TimeSpan.FromTicks(value.Ticks * factor);
        }
    }
}