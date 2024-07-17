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

using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using System.Collections.Generic;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Continuous Futures Regression algorithm. 
    /// Asserting the behavior of trailing stop order <see cref="TrailingStopOrder"/> in extended market hours 
    /// <seealso cref="Data.UniverseSelection.UniverseSettings.ExtendedMarketHours"/>
    /// </summary>
    public class FutureTrailingStopOrderOnExtendedHoursRegressionAlgorithm : QCAlgorithm, IRegressionAlgorithmDefinition
    {
        public override void Initialize()
        {
            SetStartDate(2013, 10, 6);
            SetEndDate(2013, 10, 12);

            var SP500EMini = AddFuture(Futures.Indices.SP500EMini, Resolution.Minute, extendedMarketHours: true);

            Schedule.On(DateRules.EveryDay(), TimeRules.At(19, 0), () =>
            {
                MarketOrder(SP500EMini.Mapped, 1);
                TrailingStopOrder(SP500EMini.Mapped, -1, 5, false);
            });
        }

        /// <summary>
        /// Order fill event handler. On an order fill update the resulting information is passed to this method.
        /// </summary>
        /// <param name="orderEvent">Order event details containing details of the events</param>
        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            if (IsMarketOpen(orderEvent.Symbol) && orderEvent.Status == OrderStatus.Filled)
            {
                throw new RegressionTestException($"Order filled during regular hours.");
            }
        }

        /// <summary>
        /// This is used by the regression test system to indicate if the open source Lean repository has the required data to run this algorithm.
        /// </summary>
        public bool CanRunLocally { get; } = true;

        /// <summary>
        /// This is used by the regression test system to indicate which languages this algorithm is written in.
        /// </summary>
        public List<Language> Languages { get; } = new() { Language.CSharp, Language.Python };

        /// <summary>
        /// Data Points count of all time slices of algorithm
        /// </summary>
        public long DataPoints => 75960;

        /// <summary>
        /// Data Points count of the algorithm history
        /// </summary>
        public int AlgorithmHistoryDataPoints => 0;

        /// <summary>
        /// Final status of the algorithm
        /// </summary>
        public AlgorithmStatus AlgorithmStatus => AlgorithmStatus.Completed;

        /// <summary>
        /// This is used by the regression test system to indicate what the expected statistics are from running the algorithm
        /// </summary>
        public Dictionary<string, string> ExpectedStatistics => new()
        {
            {"Total Orders", "10"},
            {"Average Win", "0.25%"},
            {"Average Loss", "-0.19%"},
            {"Compounding Annual Return", "-3.703%"},
            {"Drawdown", "0.700%"},
            {"Expectancy", "-0.061"},
            {"Start Equity", "100000"},
            {"End Equity", "99941"},
            {"Net Profit", "-0.059%"},
            {"Sharpe Ratio", "0.542"},
            {"Sortino Ratio", "1.11"},
            {"Probabilistic Sharpe Ratio", "48.279%"},
            {"Loss Rate", "60%"},
            {"Win Rate", "40%"},
            {"Profit-Loss Ratio", "1.35"},
            {"Alpha", "-0.076"},
            {"Beta", "0.196"},
            {"Annual Standard Deviation", "0.055"},
            {"Annual Variance", "0.003"},
            {"Information Ratio", "-2.76"},
            {"Tracking Error", "-2.912"},
            {"Treynor Ratio", "0.152"},
            {"Total Fees", "$21.50"},
            {"Estimated Strategy Capacity", "$29000000.00"},
            {"Lowest Capacity Asset", "ES VMKLFZIH2MTD"},
            {"Portfolio Turnover", "139.27%"},
            {"OrderListHash", "a245ae8af0b404051a770cba1c4dfe57"}
        };
    }
}
