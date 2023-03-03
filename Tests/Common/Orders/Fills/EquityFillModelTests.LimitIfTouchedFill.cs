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
using System.Collections.Generic;
using System.Linq;
using Fasterflect;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Orders.Fills;
using QuantConnect.Tests.Common.Data;

namespace QuantConnect.Tests.Common.Orders.Fills
{
    [TestFixture]
    public partial class EquityFillModelTests
    {
        [Test]
        public void PerformsLimitIfTouchedBuy()
        {
            var model = new EquityFillModel();
            var order = new LimitIfTouchedOrder(Symbols.SPY, 100, 101.5m, 100m, Noon);

            var parameters = GetFillModelParameters(Symbols.SPY, order);
            var security = parameters.Security;

            // Sets price at time zero
            security.SetLocalTimeKeeper(TimeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 102m, 102m, 102m, 102m, 100));

            // Prices above the trigger price: no trigger, no fill
            var fill = model.Fill(parameters).Single();

            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.False(order.TriggerTouched);

            // Time jump => trigger touched but not limit
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 101m, 101m, 100.5m, 101m, 100));
            security.SetMarketPrice(new QuoteBar(Noon, Symbols.SPY,
                new Bar(101m, 101m, 100.5m, 101m), 100, // Bid bar
                new Bar(101m, 101m, 100.5m, 101m), 100) // Ask bar
            );
            
            fill = model.Fill(parameters).Single();
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // Time jump => limit reached, security bought
            // |---> First, ensure that price data only triggers the order
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 101m, 101m, 100.5m, 101m, 100));
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Second, ensure that quote data is not used to fill
            security.SetMarketPrice(new QuoteBar(Noon, Symbols.SPY,
                    new Bar(100m, 100m, 100m, 100m), 100, // Bid bar
                    new Bar(100m, 100m, 100m, 100m), 100) // Ask bar
            );
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Third, ensure that fill forward data is not used to fill
            // Fill forward data is not cached (SetMarketPrice)
            var tradeBar = new TradeBar(Noon, Symbols.SPY, 101m, 101m, 99m, 99m, 100);
            var fillForwardedTradeBar = tradeBar.Clone(true);
            security.SetMarketPrice(fillForwardedTradeBar);
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Last, ensure that trade data used to fill
            security.SetMarketPrice(tradeBar);
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(order.Quantity, fill.FillQuantity);
            Assert.AreEqual(order.LimitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
        }

        [Test]
        public void PerformsLimitIfTouchedSell()
        {
            var model = new EquityFillModel();
            var order = new LimitIfTouchedOrder(Symbols.SPY, -100, 101.5m, 105m, Noon);
            var parameters = GetFillModelParameters(Symbols.SPY, order);
            var security = parameters.Security;

            // Sets price at time zero
            security.SetLocalTimeKeeper(TimeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 100m, 100m, 90m, 90m, 100));

            // Prices above the trigger price: no trigger, no fill
            var fill = model.Fill(parameters).Single();

            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.False(order.TriggerTouched);

            // Time jump => trigger touched but not limit
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 102m, 103m, 102m, 102m, 100));
            security.SetMarketPrice(new QuoteBar(Noon, Symbols.SPY,
                new Bar(101m, 102m, 100m, 100m), 100, // Bid bar
                new Bar(103m, 104m, 102m, 102m), 100) // Ask bar
            );
            
            fill = model.Fill(parameters).Single();

            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // Time jump => limit reached, security bought
            // |---> First, ensure that price data only triggers the order
            security.SetMarketPrice(new TradeBar(Noon, Symbols.SPY, 100m, 100m, 99m, 99m, 100));
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Second, ensure that quote data is not used to fill
            security.SetMarketPrice(new QuoteBar(Noon, Symbols.SPY,
                    new Bar(105m, 105m, 105m, 105m), 100, // Bid bar
                    new Bar(105m, 105m, 105m, 105m), 100) // Ask bar
            );
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Third, ensure that fill forward data is not used to fill
            // Fill forward data is not cached (SetMarketPrice)
            var tradeBar = new TradeBar(Noon, Symbols.SPY, 106m, 106m, 99m, 99m, 100);
            var fillForwardedTradeBar = tradeBar.Clone(true);
            security.SetMarketPrice(fillForwardedTradeBar);
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // |---> Last, ensure that trade data used to fill
            security.SetMarketPrice(tradeBar);
            fill = model.LimitIfTouchedFill(security, order);
            Assert.AreEqual(order.Quantity, fill.FillQuantity);
            Assert.AreEqual(order.LimitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
        }

        [TestCase(100, 290.55, 290.50)]
        [TestCase(-100, 291.45, 291.50)]
        public void PerformsLimitIfTouchedFillWithTickTradeData(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var fillModel = new EquityFillModel();

            var configTick = CreateTickConfig(Symbols.SPY);
            var equity = CreateEquity(configTick);

            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with this price
            var tradeTick = new Tick { TickType = TickType.Trade, Time = time, Value = 291m };
            equity.SetMarketPrice(tradeTick);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                new MockSubscriptionDataConfigProvider(configTick),
                Time.OneHour,
                null)).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            // Create a series of price where the last value will not trigger
            // and the fill model need to use the minimum/maximum instead
            var trades = new[] { 0m, -0.05m, 0m, 0.05m, -0.1m }
                .Select(delta => new Tick 
                    { 
                        TickType = TickType.Trade,
                        Time = time, 
                        Value = triggerPrice - delta * Math.Sign(orderQuantity)
                    })
                .ToList();

            equity.Update(trades, typeof(Tick));

            fill = fillModel.LimitIfTouchedFill(equity, order);

            // Do not fill, but trigger
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // Do not fill with quote data
            equity.SetMarketPrice(new Tick
            {
                TickType = TickType.Quote,
                Time = time,
                BidPrice = limitPrice + 0.01m,
                AskPrice = limitPrice - 0.01m
            });

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            // Fill with trade data
            equity.SetMarketPrice(new Tick
            {
                TickType = TickType.Trade,
                Time = time,
                Value = limitPrice - 0.1m * Math.Sign(orderQuantity)
            });

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
            Assert.AreEqual(order.Quantity, fill.FillQuantity);
            Assert.AreEqual(limitPrice, fill.FillPrice);
        }

        [TestCase(100, 290.55, 290.50)]
        [TestCase(-100, 291.45, 291.50)]
        public void LimitIfTouchedOrderDoesNotFillUsingQuoteBar(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var fillModel = new EquityFillModel();

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var configQuoteBar = new SubscriptionDataConfig(configTradeBar, typeof(QuoteBar));
            var configProvider = new MockSubscriptionDataConfigProvider(configQuoteBar);
            configProvider.SubscriptionDataConfigs.Add(configTradeBar);
            var equity = CreateEquity(configTradeBar);

            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with these prices
            var tradeBar = new TradeBar(time, Symbols.SPY, 291m, 291m, 291m, 291m, 12345);
            equity.SetMarketPrice(tradeBar);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                configProvider,
                Time.OneHour,
                null)).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));
            
            var quoteBar = new QuoteBar(time, Symbols.SPY, 
                new Bar(290m, 292m, 289m, 291m), 12345,
                new Bar(290m, 292m, 289m, 291m), 12345);
            equity.SetMarketPrice(quoteBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);

            // LimitIfTouched orders don't trigger with QuoteBar:
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.False(order.TriggerTouched);
        }

        [TestCase(100, 290.55, 290.50)]
        [TestCase(-100, 291.45, 291.50)]
        public void LimitIfTouchedOrderFillsUsingQuoteBarIfTriggersAndFillsInTheSameBar(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var fillModel = new EquityFillModel();
            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));
            
            var parameters = GetFillModelParameters(Symbols.SPY, order);
            var equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with these prices
            var tradeBar = new TradeBar(time, Symbols.SPY, 291m, 291m, 291m, 291m, 12345);
            equity.SetMarketPrice(tradeBar);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(parameters).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.False(order.TriggerTouched);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            // The trade bar will trigger the limit order
            tradeBar = new TradeBar(time, Symbols.SPY, 290m, 292m, 289m, 291m, 12345);
            equity.SetMarketPrice(tradeBar);

            var quoteBar = new QuoteBar(time, Symbols.SPY,
                new Bar(290m, 292m, 289m, 292m), 12345,
                new Bar(290m, 292m, 289m, 289m), 12345);
            equity.SetMarketPrice(quoteBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);

            // LimitIfTouched orders don't trigger with QuoteBar:
            Assert.AreEqual(orderQuantity, fill.FillQuantity);
            Assert.AreEqual(limitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
            Assert.True(order.TriggerTouched);
        }

        [TestCase(100, 290.50, 290.50)]
        [TestCase(-100, 291.50, 291.50)]
        public void LimitIfTouchedOrderDoesNotFillUsingTickTypeQuote(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var fillModel = new EquityFillModel();

            var configTick = CreateTickConfig(Symbols.SPY);
            var equity = CreateEquity(configTick);

            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with this price
            var tradeTick = new Tick { TickType = TickType.Trade, Time = time, Value = 291m };
            equity.SetMarketPrice(tradeTick);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                new MockSubscriptionDataConfigProvider(configTick),
                Time.OneHour,
                null)).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var price = limitPrice - 0.1m * Math.Sign(orderQuantity);
            var quoteTick = new Tick { TickType = TickType.Quote, Time = time, BidPrice = price, AskPrice = price, Value = price };
            equity.SetMarketPrice(quoteTick);
            
            fill = fillModel.LimitIfTouchedFill(equity, order);

            // LimitIfTouched orders don't trigger with TickType.Quote:
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.False(order.TriggerTouched);
        }

        [TestCase(100, 290.50, 290.50)]
        [TestCase(-100, 291.50, 291.50)]
        public void LimitIfTouchedOrderFillsUsingTickTypeQuoteIfTriggersAndFillsInTheSameBar(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var fillModel = new EquityFillModel();

            var configTick = CreateTickConfig(Symbols.SPY);
            var equity = CreateEquity(configTick);

            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with this price
            var tradeTick = new Tick { TickType = TickType.Trade, Time = time, Value = 291m };
            equity.SetMarketPrice(tradeTick);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                new MockSubscriptionDataConfigProvider(configTick),
                Time.OneHour,
                null)).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var price = limitPrice - 0.1m * Math.Sign(orderQuantity);
            var quoteTick = new Tick { TickType = TickType.Quote, Time = time, BidPrice = price, AskPrice = price, Value = price };
            tradeTick = new Tick { TickType = TickType.Trade, Time = time, Value = triggerPrice };
            equity.Update(new[] { tradeTick, quoteTick }, typeof(Tick));

            fill = fillModel.LimitIfTouchedFill(equity, order);

            // LimitIfTouched orders don't trigger with TickType.Quote:
            Assert.AreEqual(orderQuantity, fill.FillQuantity);
            Assert.AreEqual(limitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
            Assert.True(order.TriggerTouched);
        }

        [TestCase(100, 290.55, 290.50)]
        [TestCase(-100, 291.45, 291.50)]
        public void LimitIfTouchedOrderFillsAtLimitPriceWithFavorableGap(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            // See https://github.com/QuantConnect/Lean/issues/963

            var fillModel = new EquityFillModel();
            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var equity = CreateEquity(configTradeBar);

            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            // The order will not fill with these prices
            equity.SetMarketPrice(new TradeBar(time, Symbols.SPY, 291m, 291m, 291m, 291m, 12345));

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                new MockSubscriptionDataConfigProvider(configTradeBar),
                Time.OneHour,
                null)).Single();

            // Do not trigger on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));
            equity.SetMarketPrice(new TradeBar(time, Symbols.SPY, triggerPrice, triggerPrice, triggerPrice, limitPrice + 0.01m * Math.Sign(orderQuantity), 12345));

            fill = fillModel.LimitIfTouchedFill(equity, order);

            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);
            Assert.True(order.TriggerTouched);

            time += TimeSpan.FromMinutes(2);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            // The Gap TradeBar has all prices below/above the limit price 
            var gapTradeBar = Math.Sign(orderQuantity) switch
            { 
                1 => new TradeBar(time, Symbols.SPY, limitPrice - 1, limitPrice - 1, limitPrice - 2, limitPrice - 1, 12345),
                -1 => new TradeBar(time, Symbols.SPY, limitPrice + 1, limitPrice + 2, limitPrice + 1, limitPrice + 1, 12345),
            };

            equity.SetMarketPrice(gapTradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);

            // this fills worst case scenario, so it's at the limit price
            Assert.AreEqual(order.Quantity, fill.FillQuantity);
            Assert.AreEqual(limitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
        }

        [TestCase(100, 290.55, 290.50)]
        [TestCase(-100, 291.45, 291.50)]
        public void LimitIfTouchedOrderDoesNotFillUsingDataBeforeSubmitTime(decimal orderQuantity, decimal triggerPrice, decimal limitPrice)
        {
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var equity = CreateEquity(configTradeBar);

            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var tradeBar = new TradeBar(time, Symbols.SPY, 290m, 292m, 289m, 291m, 12345);
            equity.SetMarketPrice(tradeBar);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            var fillForwardBar = (TradeBar)tradeBar.Clone(true);
            equity.SetMarketPrice(fillForwardBar);

            var fillModel = new EquityFillModel();
            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var fill = fillModel.Fill(new FillModelParameters(
                equity,
                order,
                new MockSubscriptionDataConfigProvider(configTradeBar),
                Time.OneHour,
                null)).Single();

            // Do not fill on stale data
            Assert.AreEqual(0, fill.FillQuantity);
            Assert.AreEqual(0, fill.FillPrice);
            Assert.AreEqual(OrderStatus.None, fill.Status);

            time += TimeSpan.FromMinutes(1);
            timeKeeper.SetUtcDateTime(time.ConvertToUtc(TimeZones.NewYork));

            tradeBar = new TradeBar(time, Symbols.SPY, 290m, 292m, 289m, limitPrice - 0.01m * Math.Sign(orderQuantity), 12345);
            equity.SetMarketPrice(tradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);

            Assert.AreEqual(orderQuantity, fill.FillQuantity);
            Assert.AreEqual(limitPrice, fill.FillPrice);
            Assert.AreEqual(OrderStatus.Filled, fill.Status);
            Assert.AreEqual(0, fill.OrderFee.Value.Amount);
        }

        [TestCase(-100, 291.45, 291.50, 291.40, 291.50)]
        [TestCase(-100, 291.50, 291.45, 291.40, 291.45)]
        [TestCase(-100, 291.45, 291.50, 291.475, 291.50)]
        [TestCase(-100, 291.50, 291.45, 291.475, 291.45)]
        [TestCase(-100, 291.45, 291.50, 291.6, 291.6)]
        [TestCase(-100, 291.50, 291.45, 291.6, 291.6)]
        [TestCase(100, 291.55, 291.50, 291.40, 291.4)]
        [TestCase(100, 291.50, 291.55, 291.40, 291.4)]
        [TestCase(100, 291.55, 291.50, 291.525, 0)]
        [TestCase(100, 291.50, 291.55, 291.525, 0)]
        [TestCase(100, 291.55, 291.50, 291.6, 0)]
        [TestCase(100, 291.50, 291.55, 291.6, 0)]
        public void LimitIfTouchedLinearBullMarket(decimal orderQuantity, decimal triggerPrice, decimal limitPrice, decimal open, decimal excepted)
        {
            var fillModel = new EquityFillModel();
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var configProvider = new MockSubscriptionDataConfigProvider(CreateTickConfig(Symbols.SPY));
            configProvider.SubscriptionDataConfigs.Add(configTradeBar);

            var parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            var equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var trades = Enumerable.Range(0, 20)
                .Select(x => new Tick
                {
                    TickType = TickType.Trade,
                    Time = time.AddMilliseconds(x + 100),
                    Value = x / 100m + open
                }).ToList();

            equity.Update(trades, typeof(Tick));
            
            var fill = fillModel.Fill(parameters).Single();
            Assert.AreEqual(excepted, fill.FillPrice);

            // TradeBar Play
            order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var tradeBar = new TradeBar(time.AddMinutes(1), equity.Symbol, open, open, open, open, 100);
            foreach (var trade in trades)
            {
                tradeBar.UpdateTrade(trade.LastPrice, trade.Quantity);
            }
            equity.SetMarketPrice(tradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(excepted, fill.FillPrice);
        }

        [TestCase(-100, 291.45, 291.50, 291.6, 291.6)]
        [TestCase(-100, 291.50, 291.45, 291.6, 291.6)]
        [TestCase(-100, 291.45, 291.50, 291.475, 0)]
        [TestCase(-100, 291.50, 291.45, 291.475, 0)]
        [TestCase(-100, 291.45, 291.50, 291.4, 0)]
        [TestCase(-100, 291.50, 291.45, 291.4, 0)]
        [TestCase(100, 291.55, 291.50, 291.6, 291.5)]
        [TestCase(100, 291.50, 291.55, 291.6, 291.55)]
        [TestCase(100, 291.55, 291.50, 291.525, 291.5)]
        [TestCase(100, 291.50, 291.55, 291.525, 291.55)]
        [TestCase(100, 291.55, 291.50, 291.4, 291.4)]
        [TestCase(100, 291.50, 291.55, 291.4, 291.4)]
        public void LimitIfTouchedLinearBearMarket(decimal orderQuantity, decimal triggerPrice, decimal limitPrice, decimal open, decimal excepted)
        {
            var fillModel = new EquityFillModel();
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var configProvider = new MockSubscriptionDataConfigProvider(CreateTickConfig(Symbols.SPY));
            configProvider.SubscriptionDataConfigs.Add(configTradeBar);

            var parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            var equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var trades = Enumerable.Range(0,20)
                .Select(x => new Tick
                {
                    TickType = TickType.Trade,
                    Time = time.AddMilliseconds(x + 100),
                    Value = (20 - x) / 100m + open - 0.2m
                }).ToList();

            equity.Update(trades, typeof(Tick));

            var fill = fillModel.Fill(parameters).Single();
            Assert.AreEqual(excepted, fill.FillPrice);

            // TradeBar Play
            order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var tradeBar = new TradeBar(time.AddMinutes(1), equity.Symbol, open, open, open, open, 100);
            foreach (var trade in trades)
            {
                tradeBar.UpdateTrade(trade.LastPrice, trade.Quantity);
            }
            equity.SetMarketPrice(tradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(excepted, fill.FillPrice);
        }

        [TestCase(100, 291.50, 291.55, 291.4, 0.2, 291.4)]
        [TestCase(100, 291.50, 291.55, 291.6, 0.2, 291.55)]
        [TestCase(100, 291.50, 291.55, 291.525, 0.2, 291.55)]
        [TestCase(100, 291.55, 291.5, 291.4, 0.2, 291.4)]
        [TestCase(100, 291.55, 291.5, 291.6, 0.2, 291.5)]
        [TestCase(100, 291.55, 291.5, 291.525, 0.2, 291.5)]
        public void LimitIfTouchedBullZigZagHighBeforeLow(decimal orderQuantity, decimal triggerPrice, decimal limitPrice, decimal open, decimal range, decimal excepted)
        {
            var fillModel = new EquityFillModel();
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var configProvider = new MockSubscriptionDataConfigProvider(CreateTickConfig(Symbols.SPY));
            configProvider.SubscriptionDataConfigs.Add(configTradeBar);

            var parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            var equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var trades = new List<Tick>();
            var prices = new[] { open, open + range, open - range, open + range / 2 };
            var tradeBar = new TradeBar(time.AddMinutes(1), equity.Symbol, open, open, open, open, 100);
            
            for (var i = 0; i < prices.Length; i++)
            {
                trades.Add(new Tick
                {
                    TickType = TickType.Trade,
                    Time = time.AddMilliseconds(i + 100),
                    Value = prices[i]
                });
                tradeBar.UpdateTrade(prices[i], 0);
            }

            equity.Update(trades, typeof(Tick));

            var fill = fillModel.Fill(parameters).Single();
            Assert.AreEqual(excepted, fill.FillPrice);

            // TradeBar Play
            order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            equity.SetMarketPrice(tradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(excepted, fill.FillPrice);
        }

        [TestCase(100, 291.50, 291.55, 291.4, 0.2, 291.4)]
        [TestCase(100, 291.50, 291.55, 291.6, 0.2, 291.55)]
        [TestCase(100, 291.50, 291.55, 291.525, 0.2, 291.55)]
        [TestCase(100, 291.55, 291.5, 291.4, 0.2, 291.4)]
        [TestCase(100, 291.55, 291.5, 291.6, 0.2, 291.5)]
        [TestCase(100, 291.55, 291.5, 291.525, 0.2, 291.5)]
        public void LimitIfTouchedBullZigZagLowBeforeHigh(decimal orderQuantity, decimal triggerPrice, decimal limitPrice, decimal open, decimal range, decimal excepted)
        {
            var fillModel = new EquityFillModel();
            var time = new DateTime(2018, 9, 24, 9, 30, 0);
            var timeKeeper = new TimeKeeper(time.ConvertToUtc(TimeZones.NewYork), TimeZones.NewYork);

            var order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            var configTradeBar = CreateTradeBarConfig(Symbols.SPY);
            var configProvider = new MockSubscriptionDataConfigProvider(CreateTickConfig(Symbols.SPY));
            configProvider.SubscriptionDataConfigs.Add(configTradeBar);

            var parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            var equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            var trades = new List<Tick>();
            var prices = new[] { open, open - range, open + range, open + range / 2 };
            var tradeBar = new TradeBar(time.AddMinutes(1), equity.Symbol, open, open, open, open, 100);

            for (var i = 0; i < prices.Length; i++)
            {
                trades.Add(new Tick
                {
                    TickType = TickType.Trade,
                    Time = time.AddMilliseconds(i + 100),
                    Value = prices[i]
                });
                tradeBar.UpdateTrade(prices[i], 0);
            }

            equity.Update(trades, typeof(Tick));

            var fill = fillModel.Fill(parameters).Single();
            Assert.AreEqual(excepted, fill.FillPrice);

            // TradeBar Play
            order = new LimitIfTouchedOrder(Symbols.SPY, orderQuantity, triggerPrice, limitPrice, time.ConvertToUtc(TimeZones.NewYork));

            parameters = new FillModelParameters(CreateEquity(configTradeBar), order,
                configProvider, Time.OneHour, null);

            equity = parameters.Security;
            equity.SetLocalTimeKeeper(timeKeeper.GetLocalTimeKeeper(TimeZones.NewYork));

            equity.SetMarketPrice(tradeBar);

            fill = fillModel.LimitIfTouchedFill(equity, order);
            Assert.AreEqual(excepted, fill.FillPrice);
        }

    }
}
