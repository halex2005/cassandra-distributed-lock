using System;
using System.Diagnostics;

using Vostok.Metrics.Grouping;
using Vostok.Metrics.Primitives.Timer;

namespace SkbKontur.Cassandra.DistributedLock.Async.RemoteLocker
{
    public static class MetricsExtensions
    {
        public static IDisposable NewContext(this ITimer timer, Action<TimeSpan> finalAction, string userValue)
        {
            return new TimeMeasuringContext(timer, finalAction, userValue);
        }

        private struct TimeMeasuringContext : IDisposable
        {
            public TimeMeasuringContext(ITimer timer, Action<TimeSpan> finalAction, string userValue)
            {
                this.timer = timer;
                this.finalAction = finalAction;
                this.userValue = userValue;
                stopwatch = Stopwatch.StartNew();

                disposed = false;
            }

            public void Dispose()
            {
                if (!disposed)
                {
                    disposed = true;
                    stopwatch.Stop();
                    timer.Report(stopwatch.Elapsed);
                    finalAction(stopwatch.Elapsed);
                }
            }

            private readonly ITimer timer;
            private readonly Action<TimeSpan> finalAction;
            private readonly string userValue;
            private readonly Stopwatch stopwatch;

            private bool disposed;
        }
    }
}