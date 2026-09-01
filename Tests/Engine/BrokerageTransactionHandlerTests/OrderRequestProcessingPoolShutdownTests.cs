/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NUnit.Framework;
using QuantConnect.Lean.Engine.TransactionHandlers;
using QuantConnect.Orders;
using QuantConnect.Util;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace QuantConnect.Tests.Engine.BrokerageTransactionHandlerTests
{
    /// <summary>
    /// Reproduces the shutdown behaviour of <see cref="OrderRequestProcessingPool"/>: on teardown the pool completes
    /// adding and lets the workers drain, so every request queued at the moment of the stop is still delivered to the
    /// brokerage, and a worker already inside a brokerage call is never interrupted by the shutdown cancellation.
    /// </summary>
    /// <remarks>
    /// These tests document the behaviour observed on a live deploy where four market orders reached the broker
    /// between 9 and 10.5 minutes after the algorithm had stopped on a runtime error, with result reporting already
    /// ended so the fills were never shown to the user.
    /// </remarks>
    [TestFixture, Parallelizable(ParallelScope.Fixtures)]
    public class OrderRequestProcessingPoolShutdownTests
    {
        private static readonly DateTime Reference = new DateTime(2025, 07, 03, 10, 0, 0);

        [Test]
        public void QueuedRequestsAreStillSentToTheBrokerageAfterShutdownStarts()
        {
            const int orderCount = 4;

            using var brokerageCall = new ManualResetEventSlim(false);
            using var firstRequestRunning = new ManualResetEventSlim(false);
            var shuttingDown = false;
            Exception processingError = null;
            // records, per request that reached the "brokerage", whether the pool was already being torn down
            var submitted = new ConcurrentQueue<(int OrderId, bool AfterShutdownStarted)>();

            // a single worker so the requests queue up behind the one blocked on the brokerage call
            var pool = new OrderRequestProcessingPool(concurrencyEnabled: false, minimumThreads: 1, maximumThreads: 1,
                request =>
                {
                    // stand in for the brokerage call the worker is parked in while the algorithm stops
                    firstRequestRunning.Set();
                    brokerageCall.Wait();
                    submitted.Enqueue((request.OrderId, Volatile.Read(ref shuttingDown)));
                },
                exception => processingError = exception);

            try
            {
                for (var i = 1; i <= orderCount; i++)
                {
                    var submit = new SubmitOrderRequest(OrderType.Market, Symbols.SPY.SecurityType, Symbols.SPY, 1, 0, 0, Reference, "");
                    submit.SetOrderId(i);
                    pool.Dispatch(submit, Order.CreateOrder(submit));
                }

                // the first request is in flight and the other three are queued, exactly the state the pool is in
                // when the algorithm stops on a runtime error
                Assert.IsTrue(firstRequestRunning.Wait(10000), "the first request never started processing");

                // the algorithm has stopped: the engine tears the transaction handler down
                Volatile.Write(ref shuttingDown, true);
                var shutdown = Task.Run(() => pool.Dispose());

                // let the brokerage answer only once the teardown is under way. Dispose blocks joining the worker,
                // so it cannot signal its own progress, give it room to complete adding and reach the join
                Thread.Sleep(250);
                brokerageCall.Set();

                Assert.IsTrue(shutdown.Wait(30000), "the pool did not shut down");

                // every single order still reached the brokerage, all of them after the stop
                Assert.AreEqual(orderCount, submitted.Count,
                    $"expected every queued request to be drained, got: {string.Join(", ", submitted.Select(x => x.OrderId))}");
                CollectionAssert.AreEqual(Enumerable.Range(1, orderCount), submitted.Select(x => x.OrderId));
                Assert.IsTrue(submitted.All(x => x.AfterShutdownStarted),
                    "expected every request to be submitted after the shutdown had started");
                Assert.IsNull(processingError, $"the pool reported an error: {processingError}");
            }
            finally
            {
                brokerageCall.Set();
                pool.DisposeSafely();
            }
        }

        /// <summary>
        /// The shutdown budget is 60s of <see cref="Thread.Join(TimeSpan)"/> plus 60s of StopSafely per worker,
        /// applied serially, so the exposure grows linearly with the pool size and nothing observes the cancellation
        /// token mid request. Explicit because it takes over two minutes per worker by design.
        /// </summary>
        [Test, Explicit("takes ~120 seconds per worker thread by design")]
        public void ShutdownDoesNotInterruptAnInFlightBrokerageCall()
        {
            using var requestRunning = new ManualResetEventSlim(false);
            var submitted = 0;

            var pool = new OrderRequestProcessingPool(concurrencyEnabled: false, minimumThreads: 1, maximumThreads: 1,
                request =>
                {
                    requestRunning.Set();
                    // a brokerage call that outlasts the whole shutdown budget, ignoring the pool's cancellation
                    // token exactly like a blocking HTTP retry loop does
                    Thread.Sleep(TimeSpan.FromSeconds(150));
                    Interlocked.Increment(ref submitted);
                },
                // the worker outlives the pool here, so it comes back to an already disposed queue, that is expected
                _ => { });

            try
            {
                var submit = new SubmitOrderRequest(OrderType.Market, Symbols.SPY.SecurityType, Symbols.SPY, 1, 0, 0, Reference, "");
                submit.SetOrderId(1);
                pool.Dispatch(submit, Order.CreateOrder(submit));
                Assert.IsTrue(requestRunning.Wait(10000), "the request never started processing");

                var stopwatch = Stopwatch.StartNew();
                pool.Dispose();
                stopwatch.Stop();

                // the 60s Join and the 60s StopSafely both elapse without the request being cancelled, and the order
                // is submitted anyway once the brokerage answers
                Assert.GreaterOrEqual(stopwatch.Elapsed, TimeSpan.FromSeconds(120),
                    "expected the shutdown to spend its full per worker budget");
                Assert.AreEqual(0, Volatile.Read(ref submitted), "the request should still be in flight at this point");

                // the order goes out anyway, after the pool has been fully disposed
                Assert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref submitted) == 1, 60000),
                    "expected the in flight request to be submitted despite the shutdown");
            }
            finally
            {
                pool.DisposeSafely();
            }
        }
    }
}
