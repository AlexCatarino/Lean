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

using Ionic.Zip;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;

namespace QuantConnect.ToolBox.BovespaDownloader
{
    /// <summary>
    /// Bovespa Data Downloader class
    /// </summary>
    public class BovespaDataDownloader : IDataDownloader
    {
        private string _ftpsite = "ftp://ftp.bmf.com.br/MarketData/Bovespa-Vista";
        private string _inputDirectory = @"./Data/Equity";
        private const string InstrumentsFileName = "instruments_bovespa.txt";
        private Dictionary<string, LeanInstrument> _instruments = new Dictionary<string, LeanInstrument>();
        private static readonly Dictionary<SecurityType, string> FtpSiteDic = new Dictionary<SecurityType, string>() 
        { 
            { SecurityType.Future, "ftp://ftp.bmf.com.br/MarketData/BMF" },
            { SecurityType.Equity, "ftp://ftp.bmf.com.br/MarketData/Bovespa-Vista" },
            { SecurityType.Option, "ftp://ftp.bmf.com.br/MarketData/Bovespa-Opcoes" } 
        };

        public TickType DataType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BovespaDataDownloader"/> class
        /// </summary>
        public BovespaDataDownloader()
        {
            LoadInstruments();
            DataType = TickType.Trade;
        }

        /// <summary>
        /// Loads the instrument list from the instruments.txt file
        /// </summary>
        /// <returns></returns>
        private void LoadInstruments()
        {
            if (!File.Exists(InstrumentsFileName))
                throw new FileNotFoundException(InstrumentsFileName + " file not found.");

            _instruments = new Dictionary<string, LeanInstrument>();

            var lines = File.ReadAllLines(InstrumentsFileName);
            foreach (var line in lines)
            {
                var tokens = line.Split(',');
                if (tokens.Length >= 3)
                {
                    var instrument = new LeanInstrument
                    {
                        Symbol = tokens[0],
                        Name = tokens[1],
                        Type = (SecurityType)Enum.Parse(typeof(SecurityType), tokens[2]),

                    };

                    if (tokens.Length >= 4) instrument.PointValue = double.Parse(tokens[3]);
                    _instruments.Add(tokens[0], instrument);
                }
            }
        }

        /// <summary>
        /// Checks if downloader can get the data for the symbol
        /// </summary>
        /// <param name="symbol"></param>
        /// <returns>Returns true if the symbol is available</returns>
        public bool HasSymbol(string symbol)
        {
            return _instruments.ContainsKey(symbol);
        }

        /// <summary>
        /// Gets the security type for the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>The security type</returns>
        public SecurityType GetSecurityType(string symbol)
        {
            return _instruments[symbol].Type;
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="symbol">Symbol for the data we're looking for.</param>
        /// <param name="type">Security type</param>
        /// <param name="resolution">Resolution of the data request</param>
        /// <param name="startUtc">Start time of the data in UTC</param>
        /// <param name="endUtc">End time of the data in UTC</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData> Get(Symbol symbol, Resolution resolution, DateTime startUtc, DateTime endUtc)
        {
            if (!_instruments.ContainsKey(symbol.Value))
                throw new ArgumentException("Invalid symbol requested: " + symbol.Value);

            if (symbol.ID.SecurityType != SecurityType.Equity)
                throw new NotSupportedException("SecurityType not available: " + symbol.ID.SecurityType);

            if (endUtc < startUtc)
                throw new ArgumentException("The end date must be greater or equal to the start date.");

            _inputDirectory = Directory.CreateDirectory(string.Format("{0}/{1}", Config.Get("input-directory", "./Data"), symbol.ID.SecurityType)).FullName;
            _ftpsite = FtpSiteDic[symbol.ID.SecurityType];

            #region For equity daily files, use equity daily data!
            if (symbol.ID.SecurityType == SecurityType.Equity && resolution == Resolution.Daily)
            {
                for (var year = startUtc.Year; year <= endUtc.Year; year++)
                {
                    foreach (var bar in GetDailyData(symbol, year))
                    {
                        startUtc = bar.Time;
                        yield return bar;
                    }
                }
            }
            #endregion

            foreach (var file in ListTickDataFiles(symbol.ID.SecurityType, resolution, startUtc, endUtc))
            {
                // Request all ticks for a specific date
                var ticks = GetTickData(symbol, file);

                // Compress tick data to speed backtests up
                // For Quote Data, transforms level 2 to NBBO data
                ticks = CompressTickData(ticks);

                if (resolution == Resolution.Tick)
                {
                    foreach (var tick in ticks)
                    {
                        yield return tick;
                    }
                }
                else
                {
                    foreach (var bar in AggregateTicks(symbol, ticks, resolution.ToTimeSpan()))
                    {
                        yield return bar;
                    }
                }
            }
        }

        /// <summary>
        /// Get daily bars for the specified symbol from given file.
        /// If we do not have the file locally we download it from Dropbox
        /// </summary>
        /// <param name="symbol">The requested symbol</param>
        /// <param name="date">The requested date</param>
        /// <returns>An enumerable of ticks</returns>
        private IEnumerable<BaseData> GetDailyData(Symbol symbol, int year)
        {
            var localfile = string.Format("{0}/COTAHIST_A{1}.zip", _inputDirectory, year);
            var remotefile = string.Format("https://dl.dropboxusercontent.com/u/44311500/Data/Equity/COTAHIST_A{0}.zip", year);
        
            #region Download file from Dropbox if it does not exist locally
            if (!File.Exists(localfile) || new FileInfo(localfile).Length == 0 || year == DateTime.Now.Year)
            {
                using (var client = new WebClient())
                {                    
                    try { client.DownloadFile(remotefile, localfile); }
                    catch (Exception) { }
                }
            }
            #endregion

            if (!File.Exists(localfile))
            {
                Console.WriteLine("Do not have nor could download COTAHIST_A{0}.zip", year);
            }
            else
            {
                ZipFile zip;
                using (var reader = Compression.Unzip(localfile, out zip))
                {
                    var bars = new List<TradeBar>();

                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        
                        if (line.Contains(symbol.Value + " "))
                        {
                            bars.Add(new TradeBar(
                                line.Substring(2).ToDateTime(),
                                symbol,
                                Convert.ToInt64(line.Substring(56, 13)) / 100m,
                                Convert.ToInt64(line.Substring(69, 13)) / 100m,
                                Convert.ToInt64(line.Substring(82, 13)) / 100m,
                                Convert.ToInt64(line.Substring(108, 13)) / 100,
                                //Convert.ToInt64(line.Substring(152, 18)),   // QUATOT
                                Convert.ToInt64(line.Substring(170, 16)),   // VOLTOT
                                Resolution.Daily.ToTimeSpan()));
                        }
                    }

                    foreach (var bar in bars.OrderBy(x => x.Time))
                    {
                        yield return bar;
                    }
                }
            }
        }

        /// <summary>
        /// Get ticks for the specified symbool from given files.
        /// </summary>
        /// <param name="symbol">The requested symbol</param>
        /// <param name="file">File with desired data</param>
        /// <returns>An enumerable of ticks</returns>
        private IEnumerable<Tick> GetTickData(Symbol symbol, string file)
        {
            // Always get TRADE data
            foreach (var tick in GetTradeData(symbol, file).OrderBy(t => t.Time))
            {
                yield return tick;
            }

            if (DataType != TickType.Quote) yield break;
            {
                // WIP: quote is not implemented yet  
                throw new NotImplementedException();

                foreach (var tick in GetQuoteData(symbol, file).OrderBy(t => t.Time))
                {
                    yield return tick;
                }
            }
        }

        /// <summary>
        /// Get TRADE ticks for the specified symbol from given file.
        /// </summary>
        /// <param name="symbol">The requested symbol</param>
        /// <param name="file">File with desired data</param>
        /// <returns>An enumerable of ticks</returns>
        private IEnumerable<Tick> GetTradeData(Symbol symbol, string file)
        {
            var localfile = string.Empty;

            if (!GetDataFile(file, out localfile)) yield break;
            ZipFile zip;
                
            using (var toplevelreader = Compression.Unzip(localfile, out zip))
            {
                for (var i = 0; i < zip.Entries.Count; i++)
                {
                    using (var reader = new StreamReader(zip[i].OpenReader()))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (!line.Contains(symbol.Value)) continue;

                            var csv = line.Split(';');

                            yield return new Tick
                            {
                                Symbol = symbol,
                                TickType = TickType.Trade,
                                Time = DateTime.ParseExact(csv[0], "yyyy-MM-dd", null).Add(TimeSpan.Parse(csv[5])),
                                Value = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture),
                                Quantity = int.Parse(csv[4]),
                                SaleCondition = string.Format("{0} {1}", csv[8], csv[12])
                            };

                        }
                    }
                }
            }
            // Manually dispose the ZipFile object
            zip.Dispose();
        }

        /// <summary>
        /// Get QUOTE ticks for the specified symbol from given files.
        /// </summary>
        /// <param name="symbol">The requested symbol</param>
        /// <param name="file">File with desired data</param>
        /// <returns>An enumerable of ticks</returns>
        private IEnumerable<Tick> GetQuoteData(Symbol symbol, string file)
        {
            var localfile = string.Empty;
            
            foreach (var XXX in new string[] { "OFER_CPA", "OFER_VDA" })
            {
                if (GetDataFile(file.Replace("NEG", XXX), out localfile))
                {
                    ZipFile zip;
        
                    using (var toplevelreader = Compression.Unzip(localfile, out zip))
                    {
                        for (var i = 0; i < zip.Entries.Count; i++)
                        {
                            using (var reader = new StreamReader(zip[i].OpenReader()))
                            {
                                while (!reader.EndOfStream)
                                {
                                    var line = reader.ReadLine();
                                    if (!line.Contains(symbol.Value)) continue;

                                    var csv = line.Split(';');
                                    
                                    var size = int.Parse(csv[9]);
                                    var price = decimal.Parse(csv[8], NumberStyles.Any, CultureInfo.InvariantCulture);
                                    
                                    yield return new Tick
                                    {
                                        Symbol = symbol,
                                        TickType = TickType.Quote,
                                        Time = DateTime.ParseExact(csv[0], "yyyy-MM-dd", null).Add(TimeSpan.Parse(csv[6])),
                                        SaleCondition = string.Format("{0} {1} {3} {2}", csv[3], csv[4], csv[5], csv[14]),
                                        Quantity = int.Parse(csv[10]),

                                        BidSize = localfile.Contains("OFER_CPA") ? size : 0,
                                        BidPrice = localfile.Contains("OFER_CPA") ? price : 0m,

                                        AskSize = localfile.Contains("OFER_VDA") ? size : 0,
                                        AskPrice = localfile.Contains("OFER_VDA") ? price : 0m,
                                    };
                                }
                            }
                        }
                    }
                    // Manually dispose the ZipFile object
                    zip.Dispose();
                }
            }
        }

        /// <summary>
        /// Compress Tick Data
        /// Transform level 2 quote data into NBBO (National Best Bid and Offer)
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        private IEnumerable<Tick> CompressTickData(IEnumerable<Tick> ticks)
        {
            ticks = ticks.OrderBy(t => t.Time).ToList();

            var trades = ticks.Where(t => t.TickType == TickType.Trade)
                .GroupBy(t => new { t.Time, t.LastPrice })
                .Select(t => new Tick
                {
                    Symbol = t.First().Symbol,
                    TickType = TickType.Trade,
                    Time = t.Key.Time,
                    Value = t.Key.LastPrice,
                    Quantity = t.Sum(m => m.Quantity),
                    SaleCondition = string.Join(",", t.Select(x => x.SaleCondition))
                });

            // Return if only TRADE data is requested
            if (DataType == TickType.Trade) return trades.OrderBy(t => t.Time);

            var bidList = new List<Tick>();
            var askList = new List<Tick>();
            var nbboList = new List<Tick>();
            
            foreach (var quote in FilterQuotes(ticks))
            {
                if (nbboList.Count == 0) nbboList.Add(quote);
                
                if (quote.BidPrice > 0)
                {
                    if (quote.BidPrice > nbboList.Last().BidPrice)
                    {
                        var nbbo = new Tick(quote);
                        nbbo.AskPrice = nbboList.Last().AskPrice;
                        nbboList.Add(nbbo);
                    }
                    else
                    {
                        bidList.Add(quote);
                    }
                }
                if (quote.AskPrice > 0)
                {
                    if (quote.AskPrice < nbboList.Last().AskPrice || nbboList.Last().AskPrice == 0)
                    {
                        var nbbo = new Tick(quote);
                        nbbo.BidPrice = nbboList.Last().BidPrice;
                        nbboList.Add(nbbo);
                    }
                    else
                    {
                        askList.Add(quote);
                    }
                }

                var isTrade = quote.SaleCondition.Last() == '4';

                if (isTrade)
                    continue;

            }
            return trades;
        }

        /// <summary>
        /// Filter quotes: eliminate reduntand information
        /// </summary>
        /// <param name="ticks">All the ticks read so far</param>
        /// <returns>Filtered Quotes</returns>
        private static IEnumerable<Tick> FilterQuotes(IEnumerable<Tick> ticks)
        {
            var quotes = new List<Tick>();

            // Filter the quotes
            ticks.Where(t => t.TickType == TickType.Quote)

                // First, we group the quote ticks by its ID
                .GroupBy(t => t.SaleCondition.Split(' ')[0])

                // Then we eliminate those that will be cancelled/rejected/removed/expired/eliminated
                .Where(t => t.All(x =>
                {
                    var execType = int.Parse(x.SaleCondition.Split(' ').LastOrDefault());
                    return execType != 3 && execType != 7 && execType != 8 && execType != 11 && execType != 12;

                })).ToList()

                // Select only those who were traded or last updated value
                .ForEach(t =>
                {
                    if (t.Count() == 1)
                    {
                        quotes.AddRange(t);
                        return;
                    }

                    var newAndUpdates = t.Where(q => q.SaleCondition.Last() != '4').ToList();
                    var execTrade = t.OrderBy(q => q.Quantity).LastOrDefault(q => q.SaleCondition.Last() == '4');
                    if (execTrade != null) newAndUpdates = newAndUpdates.Where(q => q.Time < execTrade.Time).ToList();

                    var tmp = new List<Tick>();

                    if (newAndUpdates.Count() == 1)
                    {
                        tmp.Add(newAndUpdates.FirstOrDefault());
                    }
                    else if (newAndUpdates.Count() > 1)
                    {
                        var execNew = newAndUpdates.LastOrDefault(q => q.SaleCondition.Last() == '1');
                        var execUpdate = newAndUpdates.LastOrDefault(q => q.SaleCondition.Last() == '2');
                        if (execNew != null)
                        {
                            if (execUpdate == null) execUpdate = execNew;
                            if (execUpdate.Time > execNew.Time) execNew.Time = execUpdate.Time;
                        }
                        tmp.Add(execUpdate);
                    }

                    if (execTrade != null) tmp.Add(execTrade);

                    quotes.AddRange(tmp);

                });

            return quotes.OrderBy(q => q.Time);
        }

        /// <summary>
        /// Aggregates a list of ticks at the requested resolution
        /// </summary>
        /// <param name="symbol">Symbol</param>
        /// <param name="ticks">Input tick data</param>
        /// <param name="resolution">Output resolution</param>
        /// <returns>Enumerable of trade bars or quote bars</returns>
        internal static IEnumerable<BaseData> AggregateTicks(Symbol symbol, IEnumerable<Tick> ticks, TimeSpan resolution)
        {
            var tradegroup = ticks.Where(t => t.TickType == TickType.Trade).GroupBy(t => t.Time.RoundDown(resolution));
            var quotegroup = ticks.Where(t => t.TickType == TickType.Quote).GroupBy(t => t.Time.RoundDown(resolution));

            foreach (var g in tradegroup)
            {
                var bar = new TradeBar { Time = g.Key, Symbol = symbol, Period = resolution };
                foreach (var tick in g)
                {
                    bar.UpdateTrade(tick.LastPrice, tick.Quantity);
                }
                yield return bar;
            }

            foreach (var g in quotegroup)
            {
                var bar = new QuoteBar { Time = g.Key, Symbol = symbol, Period = resolution };
                foreach (var tick in g)
                {
                    bar.UpdateQuote(tick.BidPrice, tick.BidSize, tick.AskPrice, tick.AskSize);
                }
                yield return bar;
            }
        }

        /// <summary>
        /// List files that contains the desired data 
        /// </summary>
        /// <param name="type">Security type</param>
        /// <param name="resolution">Resolution of input data</param>
        /// <param name="startUtc">Start time of the data in UTC</param>
        /// <param name="endUtc">End time of the data in UTC</param>
        /// <returns>Enumerable of string with data for this symbol</returns>
        private IEnumerable<string> ListTickDataFiles(SecurityType type, Resolution resolution, DateTime startUtc, DateTime endUtc)
        {
            var files = new DirectoryInfo(_inputDirectory).EnumerateFiles("NEG*.zip").Select(x => x.Name).ToList();

            var request = (FtpWebRequest)WebRequest.Create(_ftpsite);
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential("anonymous", "me@home.com");

            try
            {
                using (var response = (FtpWebResponse)request.GetResponse())
                {
                    using (var responseStream = response.GetResponseStream())
                    {
                        using (var reader = new StreamReader(responseStream))
                        {
                            files.AddRange(reader.ReadToEnd().Split(Environment.NewLine.ToCharArray(), StringSplitOptions.RemoveEmptyEntries));
                        }
                    }
                }
            }
            catch (Exception) { }

            files = files.Distinct().OrderBy(x => x).ToList()
                .FindAll(x => x.Contains("NEG") && x.Contains(".zip") && !x.Contains("FRAC"))
                .FindAll(x =>
                {
                    var date = new DateTime();
                    var startdate = x.ToUpper().Contains("_A_") ? startUtc.AddDays(1 - startUtc.Day) : startUtc;
                    var didParse = DateTime.TryParseExact(x.Split('_')[type == SecurityType.Equity ? 1 : 2].Substring(0, 8), "yyyyMMdd", null, DateTimeStyles.None, out date);
                    return didParse && date >= startdate && date <= endUtc;
                });

            return files;
        }
        
        /// <summary>
        /// Download file from Bovespa FTP site
        /// </summary>
        /// <param name="file"></param>
        /// <returns>True if file exists after download</returns>
        private bool GetDataFile(string file, out string localfile)
        {
            localfile = string.Format("{0}/{1}", _inputDirectory, file);

            // Check localfile for existence and health
            if (CheckLocalFile(localfile)) return true;

            // Download remote file.
            // After remote file is downloaded, this routine is called again to check it  
            return DownloadRemoteFile(file) && GetDataFile(file, out localfile);    
        }

        /// <summary>
        /// Check local file for existence and health
        /// </summary>
        /// <param name="localfile"></param>
        /// <returns>True if file exists and is not corrupted</returns>
        private static bool CheckLocalFile(string localfile)
        {
            if (!File.Exists(localfile)) return File.Exists(localfile);

            ZipFile zip;
            Compression.Unzip(localfile, out zip);

            if (zip != null)
            {
                zip.Dispose();
            }
            else
            {
                File.Delete(localfile);
            }

            return File.Exists(localfile);
        }

        /// <summary>
        /// Download remote file
        /// </summary>
        /// <param name="file"></param>
        /// <returns>True if file was downloaded</returns>
        private bool DownloadRemoteFile(string file)
        {
            var localfile = string.Format("{0}/{1}", _inputDirectory, file);
            var remotefile = string.Format("{0}/{1}", _ftpsite, file);
            
            var request = WebRequest.Create(remotefile);
            request.Credentials = new NetworkCredential("anonymous", "me@home.com");
            request.Method = WebRequestMethods.Ftp.DownloadFile;

            try
            {
                using (var response = request.GetResponse())
                {
                    using (var responseStream = response.GetResponseStream())
                    {
                        using (var newFile = new FileStream(localfile, FileMode.Create))
                        {
                            var readCount = 0;
                            var buffer = new byte[2048];

                            while ((readCount = responseStream.Read(buffer, 0, buffer.Length)) > 0)
                                newFile.Write(buffer, 0, readCount);
                        }
                    }
                }
            }
            catch (Exception) 
            {
                Console.WriteLine("Could not download " + file);
                return false;
            }

            return true;
        }
    }
}
