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
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Securities;
using QuantConnect.Securities.Future;

namespace QuantConnect.Data.UniverseSelection
{
    /// <summary>
    /// Defines a universe for a single futures chain
    /// </summary>
    public class FuturesChainUniverse : Universe
    {
        /// <summary>
        /// True if this universe filter can run async in the data stack
        /// </summary>
        public override bool Asynchronous
        {
            get
            {
                if (UniverseSettings.Asynchronous.HasValue)
                {
                    return UniverseSettings.Asynchronous.Value;
                }
                return Future.ContractFilter.Asynchronous;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FuturesChainUniverse"/> class
        /// </summary>
        /// <param name="future">The canonical future chain security</param>
        /// <param name="universeSettings">The universe settings to be used for new subscriptions</param>
        public FuturesChainUniverse(Future future,
            UniverseSettings universeSettings)
            : base(future.SubscriptionDataConfig)
        {
            Future = future;
            UniverseSettings = universeSettings;
        }

        /// <summary>
        /// The canonical future chain security
        /// </summary>
        public Future Future { get; }

        /// <summary>
        /// Gets the settings used for subscriptons added for this universe
        /// </summary>
        public override UniverseSettings UniverseSettings
        {
            set
            {
                if (value != null)
                {
                    // make sure data mode is raw
                    base.UniverseSettings = new UniverseSettings(value) { DataNormalizationMode = DataNormalizationMode.Raw };
                }
            }
        }

        /// <summary>
        /// Performs universe selection using the data specified
        /// </summary>
        /// <param name="utcTime">The current utc time</param>
        /// <param name="data">The symbols to remain in the universe</param>
        /// <returns>The data that passes the filter</returns>
        public override IEnumerable<Symbol> SelectSymbols(DateTime utcTime, BaseDataCollection data)
        {
            var localEndTime = utcTime.ConvertFromUtc(Future.Exchange.TimeZone);
            var availableContracts = data.Data.Cast<FutureUniverse>().ToList();
            var results = Future.ContractFilter.Filter(new FutureFilterUniverse(availableContracts, localEndTime));

            return results.Select(x => x.Symbol);
        }
    }
}
