# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *
from QuantConnect import Orders

# <summary>
# This example demonstrates how to create future 'trailing_stop_order' in extended Market Hours time
# </summary>

class FutureTrailingStopOrderOnExtendedHoursRegressionAlgorithm(QCAlgorithm):
    # Keep new created instance of stop_market_order
    sp_500_e_mini = None

    # Initialize the Algorithm and Prepare Required Data
    def initialize(self):
        self.set_start_date(2013, 10, 6)
        self.set_end_date(2013, 10, 12)

        # Add mini SP500 future with extended Market hours flag
        self.sp_500_e_mini = self.add_future(Futures.Indices.SP_500_E_MINI, Resolution.MINUTE, extended_market_hours=True)

        # Init new schedule event with params: every_day, 19:00:00 PM, what should to do
        self.schedule.on(self.date_rules.every_day(),self.time_rules.at(19, 0),self.make_market_and_stop_market_order)

    # This method is opened 2 new orders by scheduler
    def make_market_and_stop_market_order(self):
        self.market_order(self.sp_500_e_mini.mapped, 1)
        self.trailing_stop_order(self.sp_500_e_mini.mapped, -1, 5, False)

    # An order fill update the resulting information is passed to this method.
    def on_order_event(self, order_event):
        if self.is_market_open(order_event.Symbol) and order_event.status == OrderStatus.FILLED:
            raise Exception(f"Order filled during regular hours.")
