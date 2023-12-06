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

using QuantConnect.Data;
using QuantConnect.Data.Market;
using System;
using System.IO;
using System.Linq;

namespace QuantConnect.ToolBox.AgileFuturesConverter
{
    public class AgileFuturesConverter
    {
        public static void Convert()
        {
            var dataDirectory = Environment.GetEnvironmentVariable("AGILE_DATA_DIRECTORY") ?? "/raw/agile";

            foreach (var ticker in new[] { "nq", "es" })
            {
                foreach (var (filename, entries) in Compression.Unzip($"{dataDirectory}/{ticker}_futures_data.zip"))
                {

                    if (entries.Count == 0 || !filename.EndsWith("csv", StringComparison.InvariantCultureIgnoreCase))
                    {
                        Logging.Log.Trace($"Invalid filename. Name: {filename} Number of rows: {entries.Count}");
                        continue;
                    }

                    // filename format example: nq_futures_data/1M_NQZ17.csv
                    var key = Path.GetFileNameWithoutExtension(filename);
                    var start = key.LastIndexOf('_');
                    if (start < 0)
                    {
                        Logging.Log.Trace($"Invalid filename. Name: {filename} Number of rows: {entries.Count}");
                        continue;
                    }

                    key = key[(1 + start)..];

                    var futureYear = 2000 + int.Parse(key[3..]);
                    if (futureYear > 2050) futureYear -= 100;
                    var contract = SymbolRepresentation.ParseFutureSymbol(key, futureYear);

                    var writer = new LeanDataWriter(Resolution.Minute, contract, dataDirectory);

                    var tradeBars = entries
                        .Select(entry =>
                        {
                            var csv = entry.Split(',');
                            if (csv.Length < 5)
                            {
                                Logging.Log.Trace($"Invalid entry. Name: {filename} Entry: {entry}");
                                return null;
                            }

                            var time = DateTime.ParseExact(csv[0], "yyyy-MM-dd HH:mm:ss", null).AddMinutes(-1);
                            return new TradeBar(
                                time.ConvertToUtc(TimeZones.Chicago),
                                contract,
                                csv[1].ToDecimal(),
                                csv[2].ToDecimal(),
                                csv[3].ToDecimal(),
                                csv[4].ToDecimal(),
                                csv[5].ToDecimal(),
                                TimeSpan.FromMinutes(1));
                        })
                        .Where(x => x != null)
                        .ToList();

                    // Empty means invalid entry and has been logged 
                    if (!tradeBars.Any()) continue;

                    writer.Write(tradeBars);

                    Logging.Log.Trace($">>> {contract.Value} :: Expiration: {contract.ID.Date:yyyyMMdd}. Start: {tradeBars.First().EndTime:yyyyMMdd}. End: {tradeBars.Last().EndTime:yyyyMMdd}");
                }
            }
        }
    }
}
