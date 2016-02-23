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

using System;
using System.Globalization;
using System.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.ToolBox.BovespaDownloader
{
    class Program
    {
        /// <summary>
        /// Primary entry point to the program
        /// </summary>
        static void Main(string[] args)
        {
            args = new string[] { "PETR4,VALE5,USIM5,BBAS3", "Tick", "20150929", "20151231" };

            if (args.Length != 4)
            {
                Console.WriteLine("Usage: BovespaDownloader SYMBOL RESOLUTION FROMDATE TODATE");
                Console.WriteLine("SYMBOLS = eg PETR4,VALE5");
                Console.WriteLine("RESOLUTION = Second/Minute/Hour/Daily/All");
                Console.WriteLine("FROMDATE = yyyymmdd");
                Console.WriteLine("TODATE = yyyymmdd");
                Console.WriteLine("Note: Daily resolution needs to be done separately. It is faster.");
                Environment.Exit(1);
            }

            try
            {
                // Load settings from command line
                var symbols = args[0].Split(',');
                var allResolutions = args[1].ToLower() == "all";
                var resolution = allResolutions ? Resolution.Tick : (Resolution)Enum.Parse(typeof(Resolution), args[1]);
                var startDate = DateTime.ParseExact(args[2], "yyyyMMdd", CultureInfo.InvariantCulture);
                var endDate = DateTime.ParseExact(args[3], "yyyyMMdd", CultureInfo.InvariantCulture);

                // Load settings from config.json
                var dataDirectory = Config.Get("data-directory", "../../../Data");

                // Download the data
                const string market = "bra";
                var downloader = new BovespaDataDownloader();

                foreach (var symbol in symbols)
                {
                    if (!downloader.HasSymbol(symbol))
                        throw new ArgumentException("The symbol " + symbol + " is not available.");
                }

                // Download and Convert TRADE data

                foreach (var symbol in symbols)
                {
                    var securityType = downloader.GetSecurityType(symbol);
                    var symbolObject = new Symbol(GetSid(symbol, securityType), symbol);
                    var data = downloader.Get(symbolObject, securityType, resolution, startDate, endDate);

                    if (allResolutions)
                    {
                        var ticks = data.Cast<Tick>().ToList();

                        // Save the data (tick resolution)
                        var writer = new LeanDataWriter(securityType, resolution, symbolObject, dataDirectory, market);
                        writer.Write(ticks);

                        // Save the data (other resolutions)
                        foreach (var res in new[] { Resolution.Second, Resolution.Minute, Resolution.Hour })
                        {
                            var resData = BovespaDataDownloader.AggregateTicks(symbolObject, ticks, res.ToTimeSpan());

                            writer = new LeanDataWriter(securityType, res, symbolObject, dataDirectory, market);
                            writer.Write(resData);
                        }
                    }
                    else
                    {
                        // Save the data (single resolution)
                        var writer = new LeanDataWriter(securityType, resolution, symbolObject, dataDirectory, market);
                        writer.Write(data);
                    }
                }
            }
            catch (Exception err)
            {
                Log.Error("BovespaDownloader(): Error: " + err.Message);
            }
        }
        static SecurityIdentifier GetSid(string symbol, SecurityType securityType)
        {
            if (securityType == SecurityType.Equity)
            {
                return SecurityIdentifier.GenerateEquity(symbol, "bra");
            }
            
            throw new NotImplementedException("The specfied security type has not been implemented yet: " + securityType);
        }
    }
}