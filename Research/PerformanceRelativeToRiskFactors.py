import warnings
warnings.filterwarnings('ignore')
from AlgorithmImports import *
from sklearn.linear_model import LinearRegression
import plotly
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import base64

class PerformanceRelativeToRiskFactors:

    def __init__(self, qb, by_sector_equity_curve, by_sector_ratio):
        self.qb = qb
        
        by_sector_equity_curve.index = by_sector_equity_curve.index.date
        self.by_sector_equity_curve = by_sector_equity_curve
        by_sector_ratio.index = by_sector_ratio.index.date
        self.by_sector_ratio = by_sector_ratio
        
        self.start_time = self.by_sector_equity_curve.index[0]
        self.end_time = self.by_sector_equity_curve.index[-1]

        self.equity_curve = self.by_sector_equity_curve.iloc[0, 0] + (self.by_sector_equity_curve - self.by_sector_equity_curve.iloc[0, 0]).sum(axis=1)
        
        self.by_factor_equity_curve, self.by_factor_summary = self._get_by_factor_curve_and_df()

    def get_charts(self):
        fig = make_subplots(
            rows=4, cols=2,
            specs=[[{"colspan": 2}, None],
                [{"type": "table"}, {"type": "table"}],
                [{}, {}],
                [{}, {"type": "pie"}]],
            shared_xaxes=False,
            subplot_titles=("Equity Curve", "Sector Analysis Summary", "Factor Analysis Summary", "Equity Curve By Sector", "Equity Curve By Factor", "Daily Sector Exposure", "Average Factor Exposure"),
            vertical_spacing=0.1
        )

        # add each trace (or traces) to its specific subplot
        equity_curve = self.get_cumulative_return_curve()
        for i in equity_curve.data :
            fig.add_trace(i, row=1, col=1)

        sector_df = self.get_sector_summary_df()
        sector_df = sector_df.applymap(lambda x: f"{x:.4f}" if x else None)
        sector_df = sector_df.reset_index()
        sector_df.columns = ["Sector"] + list(sector_df.columns)[1:]
        sector_df_fig = go.Figure(data=[go.Table(
            header=dict(values=list(sector_df.columns),
                        align='left'),
            cells=dict(values=[sector_df["Sector"], sector_df["Average Risk Factor Exposure"], sector_df["Annualized Return"], sector_df["Cumulative Return"]],
                    align='left'))
        ])
        for i in sector_df_fig.data :
            fig.add_trace(i, row=2, col=1)

        factor_df = self.get_factor_summary_df()
        factor_df = factor_df.applymap(lambda x: f"{x:.4f}" if x else None)
        factor_df = factor_df.reset_index()
        factor_df.columns = ["Factor"] + list(factor_df.columns)[1:]
        factor_df_fig = go.Figure(data=[go.Table(
            header=dict(values=list(factor_df.columns),
                        align='left'),
            cells=dict(values=[factor_df["Factor"], factor_df["Beta"], factor_df["Annualized Return"], factor_df["Cumulative Return"], factor_df["Alpha"]],
                    align='left'))
        ])
        for i in factor_df_fig.data :
            fig.add_trace(i, row=2, col=2)

        sector_equity_curve = self.get_sector_cumulative_return_curve()
        for i in sector_equity_curve.data :    
            fig.add_trace(i, row=3, col=1)

        factor_equity_curve = self.get_factor_cumulative_return_curve()
        for i in factor_equity_curve.data :    
            fig.add_trace(i, row=3, col=2)

        sector_ratio_curve = self.get_sector_ratio_curve()
        for i in sector_ratio_curve.data :    
            fig.add_trace(i, row=4, col=1)

        factor_ratio_chart = self.get_factor_ratio_chart()
        for i in factor_ratio_chart.data :    
            fig.add_trace(i, row=4, col=2)

        fig.update_layout(height=1600, width=1400, title_text="Performance Relative to Risk Factors")

        return fig

    def convert_to_base64(self, fig):
        # convert graph to JSON
        fig_json = fig.to_json()
        # convert graph to PNG and encode it
        png = plotly.io.to_image(fig)
        png_base64 = base64.b64encode(png).decode('ascii')

    def get_sector_summary_df(self):
        by_sector_ratio_sum = self.by_sector_ratio.sum()
        avg_by_sector_ratio = (self.by_sector_ratio / self.equity_curve.to_frame().values).mean(axis=0)
        avg_by_sector_ratio["Cash"] = 1. - avg_by_sector_ratio.sum() if avg_by_sector_ratio.sum() < 1 else 0

        by_sector_ret = self.by_sector_equity_curve.iloc[-1] / self.by_sector_equity_curve.iloc[0]
        total_year = (self.end_time - self.start_time).total_seconds() / 60 / 60 / 24 / 365.25
        by_sector_annual_ret = by_sector_ret ** (1 / total_year) - 1
        by_sector_cum_ret = by_sector_ret - 1

        df = pd.concat([avg_by_sector_ratio, by_sector_annual_ret, by_sector_cum_ret], axis=1)
        df.columns = ["Average Risk Factor Exposure", "Annualized Return", "Cumulative Return"]
        return df

    def get_factor_summary_df(self):
        return self.by_factor_summary

    def get_cumulative_return_curve(self):
        by_sector_curve_sum = self.by_sector_equity_curve.sum(axis=1)
        cum_ret = (by_sector_curve_sum / by_sector_curve_sum.iloc[0]).reset_index()
        cum_ret.columns = ["Time", "Value"]
        fig = px.line(cum_ret, x='Time', y='Value', title='Equity Curve')
        return fig

    def get_sector_cumulative_return_curve(self):
        melt_by_sector_curve = (self.by_sector_equity_curve / self.by_sector_equity_curve.iloc[0]).melt()
        melt_by_sector_curve.columns = ["Sector", "Cumulative Returns by Sector"]
        melt_by_sector_curve["Date"] = list(self.by_sector_equity_curve.index) * len(self.by_sector_equity_curve.columns)
        fig = px.line(melt_by_sector_curve, x='Date', y='Cumulative Returns by Sector', color='Sector', title='Equity Curve By Sector')
        return fig

    def get_factor_cumulative_return_curve(self):
        by_factor_curve_stack = self.by_factor_equity_curve.stack(0).reset_index()
        by_factor_curve_stack.columns = ["Time", "Factor", "Cumulative Returns by Factor"]
        fig = px.line(by_factor_curve_stack, x='Time', y='Cumulative Returns by Factor', color='Factor', title='Equity Curve By Factor')
        return fig

    def get_sector_ratio_curve(self):
        melt_by_sector_ratio = (self.by_sector_ratio / self.by_sector_ratio.sum()).melt()
        melt_by_sector_ratio.columns = ["Sector", "Ratio"]
        melt_by_sector_ratio["Time"] = list(self.by_sector_ratio.index) * len(self.by_sector_ratio.columns)
        fig = px.line(melt_by_sector_ratio, x='Time', y='Ratio', color='Sector', title='Daily Exposure By Sector')
        return fig

    def get_factor_ratio_chart(self):
        by_factor_df = self.by_factor_summary.iloc[:-1].reset_index()[["index", "Beta"]]
        by_factor_df.columns = ["Factor", "Beta"]
        fig = px.pie(by_factor_df, values='Beta', names='Factor')
        fig.update_layout(title='Average Risk Factor Exposures by Factors')
        return fig

    def _get_five_factors(self):
        factors = ['market_cap', 'valuation_ratios.pb_ratio']

        def filter_function(fundamentals):
            # Select the assets that have at least 1 non-NaN factor.
            filtered = []
            for f in fundamentals:
                for factor in factors:
                    value = eval(f"f.{factor}")
                    if not np.isnan(value) and value != 0:
                        filtered.append(f)
                        break

            # Select the top/bottom quantiles for the Morningstar factors.
            symbols_by_quantile = {}
            symbols = set()
            for factor in factors:
                # Sort the assets by the factor.
                sorted_by_factor = sorted(
                    [f for f in filtered if not np.isnan(eval(f"f.{factor}"))], 
                    key=lambda f: eval(f"f.{factor}")
                )
                # Select the assets in the top/bottom quantiles.
                for f in sorted_by_factor[-len(sorted_by_factor)//2:] + sorted_by_factor[:len(sorted_by_factor)//2]:
                    symbols.add(f.symbol)

            return list(symbols)

        # Get the universe constituents and their factor values.
        universe = self.qb.add_universe(filter_function)
        universe_history = self.qb.universe_history(universe, self.start_time, self.end_time)

        # Get the price history of all selected assets.
        all_symbols = set()
        for date, fundamentals in universe_history.droplevel('symbol', axis=0).items():
            for f in fundamentals:
                all_symbols.add(f.symbol)
        all_symbols = list(all_symbols)
        prices = self.qb.history(
            all_symbols, self.start_time-timedelta(380), self.end_time, Resolution.DAILY
        )['close'].unstack(0)
        momentum = (prices / prices.shift(252)).dropna(how="all")
        momentum.index = momentum.index.date
        low_vol = prices.rolling(252).std().dropna(how="all")
        low_vol.index = low_vol.index.date
        reversal = prices.pct_change(5).dropna(how="all")
        reversal.index = reversal.index.date
        daily_returns = prices.pct_change(1)[1:]
        daily_returns.index = daily_returns.index.date

        # Get the factor quantile and trailing return of each asset over time.
        rows = []
        index = []
        for date, fundamentals in universe_history.droplevel('symbol', axis=0).items():
            write_date = f.end_time.date() - timedelta(1)
            if write_date not in momentum.index or write_date not in low_vol.index or write_date not in reversal.index or write_date not in daily_returns.index:
                continue
            
            # Select assets that have data for daily returns on this day.
            fundamentals_clean = []
            for f in fundamentals:
                s = f.symbol
                # If this is the asset's first day ever trading, we can't calculate 
                # trailing returns, so just skip this asset.
                if s not in daily_returns:
                    continue
                return_ = daily_returns[s].loc[write_date]
                if np.isnan(return_):
                    continue
                fundamentals_clean.append(f)
            
            # Sort assets by fundamental factors.
            sorted_by_factor = {}
            for factor in factors:
                sorted_by_factor[factor] = [
                    f.symbol 
                    for f in sorted(
                        [f for f in fundamentals_clean if not np.isnan(eval(f"f.{factor}"))], 
                        key=lambda f: eval(f"f.{factor}")
                    )
                ]

            # get other factors rank
            day_mom = momentum.loc[write_date].dropna()
            top_mom = day_mom.nlargest(len(day_mom)//2)
            bottom_mom = day_mom.nsmallest(len(day_mom)//2)
            day_vol = low_vol.loc[write_date].dropna()
            top_low_vol = day_vol.nsmallest(len(day_vol)//2)
            bottom_low_vol = day_vol.nlargest(len(day_vol)//2)
            day_rev = reversal.loc[write_date].dropna()
            top_reversal = day_rev.nsmallest(len(day_rev)//2)
            bottom_reversal = day_rev.nlargest(len(day_rev)//2)

            # Record the factor quantile and return of each asset.
            for f in fundamentals_clean:
                s = f.symbol
                value_by_column_name = {}
                for factor_name, sorted_symbols in sorted_by_factor.items():
                    value_by_column_name[f"{factor_name}_top"] = s in sorted_symbols[-len(sorted_by_factor)//2:]
                    value_by_column_name[f"{factor_name}_bottom"] = s in sorted_symbols[:len(sorted_by_factor)//2]
                value_by_column_name["momentum_top"] = s in top_mom
                value_by_column_name["momentum_bottom"] = s in bottom_mom
                value_by_column_name["low_volatility_top"] = s in top_low_vol
                value_by_column_name["low_volatility_bottom"] = s in bottom_low_vol
                value_by_column_name["short_term_reversal_top"] = s in top_reversal
                value_by_column_name["short_term_reversal_bottom"] = s in bottom_reversal
                value_by_column_name['return'] = daily_returns[s].loc[write_date]

                rows.append(value_by_column_name)
                index.append((f.end_time, s))

        # Organize the factor quantile and trailing return data into a DataFrame.
        columns = []
        for factor in factors+["momentum", "low_volatility", "short_term_reversal"]:
            columns.append(factor + '_top')
            columns.append(factor + '_bottom')
        columns.append('return')
        result = pd.DataFrame(
            rows, 
            columns=columns, 
            index=pd.MultiIndex.from_tuples(index, names=['end_time', 'symbol'])
        )

        # Define a method to calculate the factor return streams.
        def factor_return_stream(factor_name, inverse=False):
            return (-1 if inverse else 1) * (
                result[result[f'{factor_name}_top']]['return'].groupby(level=0).mean()
                - result[result[f'{factor_name}_bottom']]['return'].groupby(level=0).mean()
            )

        # Define our final factors.
        size_factor = factor_return_stream('market_cap', True)
        value_factor = factor_return_stream('valuation_ratios.pb_ratio', True)
        momentum_factor = factor_return_stream('momentum', True)
        low_volatility_factor = factor_return_stream('low_volatility', True)
        short_term_reversal_factor = factor_return_stream('short_term_reversal', True)
        factor_ret = pd.concat([size_factor, value_factor, momentum_factor, low_volatility_factor, short_term_reversal_factor], axis=1)
        factor_ret = (1 + factor_ret).cumprod()
        factor_ret.columns = ["Size", "Value", "Momentum", "Low Volatility", "Short Term Reversal"]
        return factor_ret

    def _get_by_factor_curve_and_df(self):
        by_factor_ret = self._get_five_factors().loc[self.start_time:self.end_time]
        by_factor_ret.index = by_factor_ret.index.date
        by_factor_ret["Portfolio"] = self.equity_curve.loc[by_factor_ret.index[0]:by_factor_ret.index[-1]]
        by_factor_ret = by_factor_ret.ffill().pct_change().dropna()

        # linear regression to obtain ratio and series
        lr = LinearRegression().fit(by_factor_ret.iloc[:, :-1], by_factor_ret.iloc[:, -1])
        by_factor_curve = (by_factor_ret.iloc[:, :-1] * lr.coef_ + 1).cumprod()

        total_year = (self.end_time - self.start_time).total_seconds() / 60 / 60 / 24 / 365.25
        by_factor_ret_last = (by_factor_ret.iloc[:, :-1] * lr.coef_ + 1).cumprod().iloc[-1] - 1
        by_factor_annual_ret = (by_factor_ret_last + 1) ** (1 / total_year) - 1
        alpha_factor = pd.Series([lr.intercept_], index=["Alpha"])
        df = pd.concat([pd.Series(lr.coef_, index=by_factor_ret.columns[:-1]), by_factor_annual_ret, by_factor_ret_last, alpha_factor], axis=1)
        df.columns = ["Beta", "Annualized Return", "Cumulative Return", "Alpha"]

        return by_factor_curve, df