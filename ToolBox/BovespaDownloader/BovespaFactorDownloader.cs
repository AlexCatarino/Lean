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
    /// Bovespa Factor Downloader class
    /// </summary>
    public class BovespaFactorDownloader : IDataDownloader
    {
        private static string _inputDirectory = string.Empty;
        private static string _ftpsite = "ftp://ftp.bmf.com.br/MarketData/";
        private string _httpssite = "https://dl.dropboxusercontent.com/u/44311500/Data/";
        private const string CodesFileName = "codes_bovespa.txt";
        private Dictionary<string, string> _codes = new Dictionary<string, string>();
        private const string InstrumentsFileName = "instruments_bovespa.txt";
        private Dictionary<string, LeanInstrument> _instruments = new Dictionary<string, LeanInstrument>();
        
        /// <summary>
        /// Initializes a new instance of the <see cref="BovespaFactorDownloader"/> class
        /// </summary>
        public BovespaFactorDownloader()
        {
            LoadInstruments();
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

            if (symbol.ID.SecurityType != SecurityType.Equity && symbol.ID.SecurityType != SecurityType.Future)   // Options do not have factors
                throw new NotSupportedException("SecurityType not available: " + symbol.ID.SecurityType);

            if (endUtc < startUtc)
                throw new ArgumentException("The end date must be greater or equal to the start date.");

            _inputDirectory = Directory.CreateDirectory(string.Format("{0}/{1}", Config.Get("input-directory", "./Data"), type)).FullName;

            GetFactors(symbol.Value);

            var remotefiles = ListRemoteFiles(symbol.ID.SecurityType, startUtc, endUtc);

            foreach (var remotefile in remotefiles)
            {
                // Request all ticks for a specific date
                var ticks = GetTicks(symbol, remotefile);

                if (resolution == Resolution.Tick)
                {
                    foreach (var tick in ticks)
                    {
                        yield return tick;
                    }
                }
                else
                {
                    foreach (var bar in AggregateTicks(symbol, ticks, resolution))
                    {
                        yield return bar;
                    }
                }
            }
        }

        private void GetFactors(string symbol)
        {
            var code = _instruments[symbol].PointValue;
            if (code == 0) return;

            var _factors = new Dictionary<string, LeanFactor>();

            var url = "http://www.bmfbovespa.com.br";
            var url_kind = "{0}/cias-listadas/empresas-listadas/ResumoProventosDinheiro.aspx?codigoCvm={1:F0}";
            var url_symb = "{0}/cias-listadas/empresas-listadas/ResumoEventosCorporativos.aspx?codigoCvm={1:F0}";
            var url_info = "{0}/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM={1:F0}";
            var kind = string.Format(">{0}<", new Dictionary<string, string> { { "3", "ON" }, { "4", "PN" }, { "5", "PNA" }, { "6", "PNB" }, { "7", "PNC" }, { "8", "PND" }, { "11", "UNT" } }[symbol.ToString().Substring(4)]);

            using (var client = new WebClient())
            {
                var factor0 = client.DownloadString(string.Format(url_kind, url, code));

                if (factor0.Contains(kind))
                {
                    foreach (var idx in AllIndexesOf(factor0, kind))
                    {
                        var price = 0m;
                        var date = new DateTime();
                        var cols = new List<string>();
                        var line = factor0.Substring(idx, factor0.IndexOf("</tr>", idx) - idx);
                        
                        foreach (var id in AllIndexesOf(line, "\">"))
                        {
                            cols.Add(line.Substring(id + 2, line.IndexOf("<", id) - id - 2));
                        }
   
                        if (!decimal.TryParse(cols[5], NumberStyles.Any, null, out price) || price <= 0) continue;

                        if (!DateTime.TryParseExact(cols[4], "dd/MM/yyyy", null, DateTimeStyles.None, out date) &&
                            !DateTime.TryParseExact(cols[3], "dd/MM/yyyy", null, DateTimeStyles.None, out date))
                            date = DateTime.ParseExact(cols[0], "dd/MM/yyyy", null, DateTimeStyles.None);

                        
                        //if (!comprice.ContainsKey(date)) comprice.Add(date, price);

                        //if (dividend.ContainsKey(date))
                        //    dividend[date] += decimal.Parse(cols[1], _ptBR);
                        //else
                        //    dividend.Add(date, decimal.Parse(cols[1], _ptBR));

                    }

                    
                }
                else
                {
                    factor0 = client.DownloadString(string.Format(url_info, url, code));
                }
            
            }



            throw new NotImplementedException();
        }

        public void WriteInstrumentsFile()
        {
            _inputDirectory = Directory.CreateDirectory(string.Format("{0}/{1}", Config.Get("input-directory", "./Data"), "Equity")).FullName;
            var dic_kind = new Dictionary<string, string> { { "3", "ON" }, { "4", "PN" }, { "5", "PNA" }, { "6", "PNB" }, { "7", "PNC" }, { "8", "PND" }, { "11", "UNT" } };
            
            for (var year = DateTime.Now.Year; year > 1998 ; year--)
            {
                var localfile = string.Format("{0}/COTAHIST_A{1}.zip", _inputDirectory, year);
                Console.Write("\rCOTAHIST_A{0}.zip", year);

                #region Download file from Dropbox if it does not exist locally
                if (!File.Exists(localfile) || new FileInfo(localfile).Length == 0 || year == DateTime.Now.Year)
                {
                    using (var client = new WebClient())
                    {
                        try { client.DownloadFile(string.Format("{0}/Equity/COTAHIST_A{1}.zip", _httpssite, year), localfile); }
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
                        while(!reader.EndOfStream)
                        {
                            int y;
                            var line = reader.ReadLine();

                            var spec = line.Substring(39, 10).Replace("*", " ").Trim().Split(' ')[0];
                            if (!dic_kind.ContainsValue(spec)) continue;

                            var symbol = line.Substring(12, 12).Trim();
                            if (HasSymbol(symbol) || !dic_kind.ContainsKey(symbol.Substring(4)) || symbol.Contains(" ")) continue;

                            var instrument = new LeanInstrument
                            {
                                Symbol = symbol,
                                Name = string.Format("{0} {1}", line.Substring(27, 12).Trim(), spec),
                                Type = SecurityType.Equity,
                            };

                            var code = _instruments.FirstOrDefault(t => t.Key.Contains(symbol.Substring(0, 4)));
                            if (code.Key != null && code.Value.PointValue > 0) instrument.PointValue = code.Value.PointValue;

                            _instruments.Add(symbol, instrument);
                        }
                    }
                }
            }
            File.WriteAllLines(InstrumentsFileName, _instruments.Select(x =>
                {
                    var line = string.Format("{0},{1},{2}", x.Key, x.Value.Name, x.Value.Type);
                    if (x.Value.PointValue > 0) line += string.Format(",{0}", x.Value.PointValue);
                    return line;
                }).OrderBy(x => x));
        }

        /// <summary>
        /// Get CVM code for given symbol 
        /// </summary>
        /// <param name="symbol">Symbol we want the code for</param>
        /// <returns>Code of given symbol</returns>
        public void LoadCodes()
        {
            var instruments = new List<string>();
            var url = "http://www.bmfbovespa.com.br";
            var url_name = "{0}/cias-listadas/empresas-listadas/BuscaEmpresaListada.aspx?Letra={1}";
            var url_kind = "{0}/cias-listadas/empresas-listadas/ResumoProventosDinheiro.aspx?codigoCvm={1}";
            var url_symb = "{0}/cias-listadas/empresas-listadas/ResumoEventosCorporativos.aspx?codigoCvm={1}";
            var url_info = "{0}/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM={1}";
            var dic_kind = new Dictionary<string, string> { { "3", "ON" }, { "4", "PN" }, { "5", "PNA" }, { "6", "PNB" }, { "7", "PNC" }, { "8", "PND" }, { "11", "UNT" } };
            
            foreach (var letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
            {
                try
                {
                    using (var client = new WebClient())
                    {
                        var content = client.DownloadString(string.Format(url_name, url, letter));
                        var idx = 0;
                        var code = 0;

                        while ((idx = content.IndexOf("Cvm=", idx) + 4) > 4)
                        {
                            if (!int.TryParse(content.Substring(idx, 6), out code) &&
                                !int.TryParse(content.Substring(idx, 5), out code) &&
                                !int.TryParse(content.Substring(idx, 4), out code) &&
                                !int.TryParse(content.Substring(idx, 3), out code))
                                code = int.Parse(content.Substring(idx, 2));

                            idx = 2 + code.ToString().Length + content.IndexOf(code.ToString(), idx + 1);                                 
                            var name = content.Substring(idx);
                            name = name.Substring(0, name.IndexOf("<"));

                            var id = 0;
                            var symbol = client.DownloadString(string.Format(url_symb, url, code));
                            if ((id = symbol.IndexOf("CodigoValor")) < 0) continue;
                            
                            symbol = symbol.Substring(1 + symbol.IndexOf(">", id), 4);

                            var info = true;
                            var kind = client.DownloadString(string.Format(url_kind, url, code));

                            foreach (var kvp in dic_kind)
                            {
                                if (!kind.Contains(string.Format(">{0}<", kvp.Value))) continue;
                                instruments.Add(string.Format("{0}{1},{2} {3},Equity,{4}", symbol, kvp.Key, name, kvp.Value, code));
                                info = false;
                            }

                            if (info)
                            {
                                var symbols = client.DownloadString(string.Format(url_info, url, code));
                                if ((id = symbols.IndexOf("Papel=") + 6) < 6) continue;
                                symbols = (symbols = symbols.Substring(id)).Substring(0, symbols.IndexOf("&"));

                                Console.WriteLine("{0}\t{1}", code, symbols);

                                foreach (var key in symbols.Replace(symbol, "").Split(','))
                                {
                                    if (!dic_kind.ContainsKey(key)) continue;
                                    instruments.Add(string.Format("{0}{1},{2} {3},Equity,{4}", symbol, key, name, dic_kind[key], code));
                                }
                            }
                        }
                    } 
                }
                catch (Exception exception) 
                {
                    Log.Error(exception.Message);
                }
            }
            File.WriteAllLines(InstrumentsFileName, instruments.OrderBy(x => x));
        }

        /// <summary>
        /// List files that contains the desired data 
        /// </summary>
        /// <param name="type">Security type</param>
        /// <param name="startUtc">Start time of the data in UTC</param>
        /// <param name="endUtc">End time of the data in UTC</param>
        /// <returns>Enumerable of string with data for this symbol</returns>
        private static List<string> ListRemoteFiles(SecurityType type, DateTime startUtc, DateTime endUtc)
        {
            var remotefiles = new List<string>();
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
                            var startdate = startUtc.AddDays(1 - startUtc.Day);
                            remotefiles = reader.ReadToEnd().Split(Environment.NewLine.ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList<string>()
                                .FindAll(x => !x.Contains("FRAC") && x.Contains(".zip") && x.Contains("NEG"))
                                .FindAll(x =>
                                {
                                    try
                                    {
                                        var datestr = x.Replace(".zip", "").Trim().Split('_')[(type == SecurityType.Equity ? 1 : 2)];
                                        var date = DateTime.ParseExact(datestr, "yyyyMMdd", null);
                                        return date >= startdate && date <= endUtc;
                                    }
                                    catch (Exception)
                                    {
                                        Log.Error(x + " is out of place!");
                                        return false;
                                    }
                                });
                        }
                    }
                }


            }
            catch (Exception)
            {

                remotefiles = new DirectoryInfo(_inputDirectory).EnumerateFiles("NEG*.zip").Where(x => x.Length > 0).Select(x => x.Name).ToList();
            }

            return remotefiles;
        }

        /// <summary>
        /// Get ticks for the specified file.
        /// If we do not have the file locally we download it.
        /// </summary>
        /// <param name="symbol">The requested symbol</param>
        /// <param name="date">The requested date</param>
        /// <returns>An enumerable of ticks</returns>
        private IEnumerable<Tick> GetTicks(Symbol symbol, string file)
        {
            var localfile = string.Format("{0}/{1}", _inputDirectory, file);

            #region Download file from FTP site if it does not exist locally
            if (!File.Exists(localfile) || new FileInfo(localfile).Length == 0)
            {
                using (var newFile = new FileStream(localfile, FileMode.Create))
                {
                    var request = (FtpWebRequest)WebRequest.Create(string.Format("{0}/{1}", _ftpsite, file));
                    request.Credentials = new NetworkCredential("anonymous", "me@home.com");
                    request.Method = WebRequestMethods.Ftp.DownloadFile;

                    using (var response = (FtpWebResponse)request.GetResponse())
                    {
                        using (var responseStream = response.GetResponseStream())
                        {
                            var readCount = 0;
                            var buffer = new byte[2048];

                            try
                            {
                                while ((readCount = responseStream.Read(buffer, 0, buffer.Length)) > 0)
                                    newFile.Write(buffer, 0, readCount);   // Write file
                            }
                            catch (Exception exception)
                            {
                                Log.Error(exception.Message);
                            }
                        }
                    }
                }
            }
            #endregion

            if (!File.Exists(localfile))
            {
                Console.WriteLine("Do not have nor could download " + localfile);
            }
            else
            {
                ZipFile zip;
                var isBid = localfile.Contains("OFER_CPA");
                var isAsk = localfile.Contains("OFER_VDA");
                var tickType = localfile.Contains("NEG") ? TickType.Trade : TickType.Quote;

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
                                var value = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture);
                                var quantity = int.Parse(csv[4]);

                                yield return new Tick
                                {
                                    Symbol = symbol,
                                    TickType = tickType,
                                    Time = DateTime.ParseExact(csv[0], "yyyy-MM-dd", null).Add(TimeSpan.Parse(csv[5])),

                                    Value = tickType == TickType.Trade ? value : 0m,
                                    BidPrice = isBid ? value : 0m,
                                    AskPrice = isAsk ? value : 0m,

                                    Quantity = tickType == TickType.Trade ? quantity : 0,
                                    BidSize = isBid ? quantity : 0,
                                    AskSize = isAsk ? quantity : 0,
                                };
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Aggregates a list of ticks at the requested resolution
        /// </summary>
        /// <param name="symbol">Symbol</param>
        /// <param name="ticks">Input tick data</param>
        /// <param name="resolution">Output resolution</param>
        /// <returns>Enumerable of trade bars or quote bars</returns>
        private static IEnumerable<BaseData> AggregateTicks(Symbol symbol, IEnumerable<Tick> ticks, Resolution resolution)
        {
            var interval = new Dictionary<Resolution, TimeSpan>()
            {
                { Resolution.Second, new TimeSpan(0, 0, 0, 1)},
                { Resolution.Minute, new TimeSpan(0, 0, 1, 0)},
                { Resolution.Hour,   new TimeSpan(0, 1, 0, 0)},
                { Resolution.Daily,  new TimeSpan(1, 0, 0, 0)}
            }[resolution];

            if (ticks.All(t => t.TickType == TickType.Trade))
            {
                return
                    (from t in ticks
                     group t by t.Time.RoundDown(interval)
                         into g
                         select new TradeBar
                         {
                             Symbol = symbol,
                             Time = g.Key,
                             Open = g.First().LastPrice,
                             High = g.Max(t => t.LastPrice),
                             Low = g.Min(t => t.LastPrice),
                             Close = g.Last().LastPrice,
                             Volume = g.Sum(t => t.Quantity)
                         });
            }
            else
            {
                return
                    (from t in ticks
                     group t by t.Time.RoundDown(interval)
                         into g
                         select new TradeBar // Substitute for QuoteBar
                         {
                             Symbol = symbol,
                             Time = g.Key,
                             Open = g.First().LastPrice,
                             High = g.Max(t => t.LastPrice),
                             Low = g.Min(t => t.LastPrice),
                             Close = g.Last().LastPrice,
                             Volume = g.Sum(t => t.Quantity)
                         });
            }
        }

        public static IEnumerable<int> AllIndexesOf(string str, string value)
        {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentException("the string to find may not be empty", "value");
            for (int index = 0; ; index += value.Length)
            {
                index = str.IndexOf(value, index);
                if (index == -1)
                    break;
                yield return index;
            }
        }
    }
}
