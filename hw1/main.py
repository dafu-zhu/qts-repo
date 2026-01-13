"""
Dafu Zhu; 12504076

Future pairs:
- CL versus HO
- YM versus RTY
"""
import os
from dotenv import load_dotenv
import wrds
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

WRDS_USERNAME = os.getenv("WRDS_USERNAME")
WRDS_PASSWORD = os.getenv("WRDS_PASSWORD")

# Analysis parameters
START_DATE = '2025-12-12'
END_DATE = '2025-12-19'  # Third Friday of December 2025
ROLLING_WINDOWS = [3, 5, 10, 20]  # N-day rolling windows for analysis

class FuturesSpreadAnalyzer:
    """Analyzes futures spread dynamics for calendar spreads."""

    def __init__(self, username, password):
        """Initialize WRDS connection."""
        print("Connecting to WRDS...")
        self.db = wrds.Connection(wrds_username=username, wrds_password=password)
        print("Connected successfully!")

    def download_futures_data(self, ticker, start_date, end_date):
        """
        Download futures data from WRDS Thomson Reuters Datastream for a given ticker.

        Args:
            ticker: Futures ticker symbol ('CL', 'HO', 'YM', 'RTY')
            start_date: Start date for data
            end_date: End date for data

        Returns:
            DataFrame with futures data including contract info
        """
        print(f"\nDownloading data for {ticker}...")

        # Contract codes mapping (from Thomson Reuters Datastream)
        contract_mapping = {
            'CL': 1986,  # Crude Oil (Light Sweet)
            'HO': 2029,  # Heating Oil (New York)
            'YM': 4712,  # Micro E-Mini Dow Jones
            'RTY': 4396  # CME E-mini Russell 2000
        }

        if ticker not in contract_mapping:
            print(f"Unknown ticker: {ticker}")
            return None

        contrcode = contract_mapping[ticker]

        # Query to get all contracts for this ticker that overlap with our date range
        query = f"""
        SELECT
            c.futcode,
            c.contrcode,
            c.dsmnem,
            c.contrname,
            c.lasttrddate,
            c.expirationdate,
            c.startdate,
            v.date_,
            v.open_,
            v.high,
            v.low,
            v.settlement as close,
            v.volume,
            v.openinterest
        FROM tr_ds_fut.wrds_contract_info c
        INNER JOIN tr_ds_fut.wrds_fut_contract v ON c.futcode = v.futcode
        WHERE c.contrcode = {contrcode}
        AND v.date_ >= '{start_date}'
        AND v.date_ <= '{end_date}'
        AND c.startdate <= '{end_date}'
        AND c.lasttrddate >= '{start_date}'
        ORDER BY v.date_, c.lasttrddate
        """

        try:
            df = self.db.raw_sql(query)
            print(f"Downloaded {len(df)} rows for {ticker}")
            if len(df) > 0:
                # Rename columns for consistency
                df = df.rename(columns={'date_': 'date', 'open_': 'open'})
                print(f"  Found {df['futcode'].nunique()} unique contracts")
                print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
            return df
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def identify_top_contracts(self, df, n_contracts=2):
        """
        Identify the top N contracts by number of data points.

        Args:
            df: DataFrame with futures data
            n_contracts: Number of contracts to identify

        Returns:
            List of futcodes for top contracts
        """
        if df is None or len(df) == 0:
            return []

        # Count rows per contract (futcode)
        contract_counts = df.groupby('futcode').agg({
            'date': 'count',
            'dsmnem': 'first',
            'lasttrddate': 'first'
        }).rename(columns={'date': 'count'})
        contract_counts = contract_counts.sort_values('count', ascending=False)

        print(f"\nTop contracts by data points:")
        print(contract_counts.head(10))

        top_contracts = contract_counts.head(n_contracts).index.tolist()
        return top_contracts

    def prepare_contract_data(self, df, futcode):
        """
        Prepare data for a specific contract with forward-fill.

        Args:
            df: DataFrame with futures data
            futcode: Futcode of the contract

        Returns:
            Series with close prices, forward-filled
        """
        if df is None or futcode is None:
            return None

        contract_data = df[df['futcode'] == futcode].copy()
        if len(contract_data) == 0:
            print(f"  Warning: No data for futcode {futcode}")
            return None

        # Ensure date column is datetime without time component
        contract_data['date'] = pd.to_datetime(contract_data['date']).dt.date
        contract_data = contract_data.set_index('date')['close'].sort_index()

        # Create full date range (as date objects, not datetime)
        date_range = pd.date_range(start=START_DATE, end=END_DATE, freq='D').date
        contract_data = contract_data.reindex(date_range)

        # Forward fill on days where data exists
        contract_data = contract_data.ffill()

        print(f"  Futcode {futcode}: {contract_data.notna().sum()} data points")

        return contract_data

    def calculate_calendar_spread(self, second_month, front_month):
        """
        Calculate calendar spread: second_month - front_month.

        Args:
            second_month: Series with second month prices
            front_month: Series with front month prices

        Returns:
            Series with spread values
        """
        if second_month is None or front_month is None:
            return None

        spread = second_month - front_month
        return spread

    def analyze_spread_dynamics(self, spread, label):
        """
        Analyze spread dynamics with rolling averages and deviations.

        Args:
            spread: Series with spread values
            label: Label for the spread (e.g., 'CL spread')

        Returns:
            Dictionary with analysis results
        """
        if spread is None or len(spread) == 0:
            return None

        results = {
            'label': label,
            'spread': spread,
            'stats': {},
            'deviations': {}
        }

        # Basic statistics
        results['stats']['mean'] = spread.mean()
        results['stats']['median'] = spread.median()
        results['stats']['std'] = spread.std()
        results['stats']['min'] = spread.min()
        results['stats']['max'] = spread.max()
        results['stats']['quantiles'] = spread.quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])

        # Rolling average deviations for different N values
        for N in ROLLING_WINDOWS:
            rolling_avg = spread.rolling(window=N, min_periods=1).mean()
            deviation = spread - rolling_avg

            results['deviations'][f'd_{N}'] = {
                'values': deviation,
                'median': deviation.median(),
                'std': deviation.std(),
                'quantiles': deviation.quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
            }

        return results

    def analyze_cross_spread_dynamics(self, spread1, spread2, label1, label2):
        """
        Analyze dynamics between two spreads.

        Args:
            spread1: First spread series
            spread2: Second spread series
            label1: Label for first spread
            label2: Label for second spread

        Returns:
            Dictionary with cross-analysis results
        """
        results = {
            'correlation': None,
            'd_correlations': {}
        }

        if spread1 is None or spread2 is None:
            return results

        # Correlation between spreads
        results['correlation'] = spread1.corr(spread2)

        # Correlations between deviation values
        for N in ROLLING_WINDOWS:
            rolling_avg1 = spread1.rolling(window=N, min_periods=1).mean()
            deviation1 = spread1 - rolling_avg1

            rolling_avg2 = spread2.rolling(window=N, min_periods=1).mean()
            deviation2 = spread2 - rolling_avg2

            results['d_correlations'][f'd_{N}'] = deviation1.corr(deviation2)

        return results

    def create_visualizations(self, results1, results2, cross_results, output_dir='output'):
        """
        Create comprehensive visualizations of the analysis.

        Args:
            results1: Analysis results for first pair
            results2: Analysis results for second pair
            cross_results: Cross-analysis results
            output_dir: Directory to save plots
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Set style
        sns.set_style('whitegrid')
        plt.rcParams['figure.figsize'] = (15, 10)

        # 1. Time series of spreads
        fig, axes = plt.subplots(2, 1, figsize=(15, 10))

        if results1:
            results1['spread'].plot(ax=axes[0], label=results1['label'], linewidth=2)
            axes[0].set_title(f"{results1['label']} Over Time", fontsize=14, fontweight='bold')
            axes[0].set_ylabel('Spread Value', fontsize=12)
            axes[0].legend()
            axes[0].grid(True, alpha=0.3)

        if results2:
            results2['spread'].plot(ax=axes[1], label=results2['label'], linewidth=2, color='orange')
            axes[1].set_title(f"{results2['label']} Over Time", fontsize=14, fontweight='bold')
            axes[1].set_xlabel('Date', fontsize=12)
            axes[1].set_ylabel('Spread Value', fontsize=12)
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/spreads_timeseries.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"\nSaved: {output_dir}/spreads_timeseries.png")

        # 2. Distribution of spreads
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        if results1:
            axes[0].hist(results1['spread'].dropna(), bins=30, alpha=0.7, edgecolor='black')
            axes[0].axvline(results1['stats']['median'], color='red', linestyle='--',
                           label=f"Median: {results1['stats']['median']:.4f}")
            axes[0].set_title(f"Distribution of {results1['label']}", fontsize=14, fontweight='bold')
            axes[0].set_xlabel('Spread Value', fontsize=12)
            axes[0].set_ylabel('Frequency', fontsize=12)
            axes[0].legend()
            axes[0].grid(True, alpha=0.3)

        if results2:
            axes[1].hist(results2['spread'].dropna(), bins=30, alpha=0.7,
                        color='orange', edgecolor='black')
            axes[1].axvline(results2['stats']['median'], color='red', linestyle='--',
                           label=f"Median: {results2['stats']['median']:.4f}")
            axes[1].set_title(f"Distribution of {results2['label']}", fontsize=14, fontweight='bold')
            axes[1].set_xlabel('Spread Value', fontsize=12)
            axes[1].set_ylabel('Frequency', fontsize=12)
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/spreads_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_dir}/spreads_distribution.png")

        # 3. Deviations for different rolling windows
        if results1 and results1['deviations']:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            axes = axes.flatten()

            for idx, N in enumerate(ROLLING_WINDOWS):
                if f'd_{N}' in results1['deviations']:
                    dev_data = results1['deviations'][f'd_{N}']['values']
                    dev_data.plot(ax=axes[idx], label=f'{N}-day deviation', linewidth=2)
                    axes[idx].axhline(0, color='red', linestyle='--', alpha=0.5)
                    axes[idx].set_title(f"{results1['label']} - Deviation from {N}-Day MA",
                                      fontsize=12, fontweight='bold')
                    axes[idx].set_ylabel('Deviation', fontsize=10)
                    axes[idx].legend()
                    axes[idx].grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(f'{output_dir}/spread1_deviations.png', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_dir}/spread1_deviations.png")

        if results2 and results2['deviations']:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            axes = axes.flatten()

            for idx, N in enumerate(ROLLING_WINDOWS):
                if f'd_{N}' in results2['deviations']:
                    dev_data = results2['deviations'][f'd_{N}']['values']
                    dev_data.plot(ax=axes[idx], label=f'{N}-day deviation',
                                 linewidth=2, color='orange')
                    axes[idx].axhline(0, color='red', linestyle='--', alpha=0.5)
                    axes[idx].set_title(f"{results2['label']} - Deviation from {N}-Day MA",
                                      fontsize=12, fontweight='bold')
                    axes[idx].set_ylabel('Deviation', fontsize=10)
                    axes[idx].legend()
                    axes[idx].grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(f'{output_dir}/spread2_deviations.png', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_dir}/spread2_deviations.png")

        # 4. Scatter plot of spreads
        if results1 and results2:
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.scatter(results1['spread'], results2['spread'], alpha=0.6, s=100)
            ax.set_xlabel(results1['label'], fontsize=12)
            ax.set_ylabel(results2['label'], fontsize=12)
            ax.set_title(f"Scatter Plot: {results1['label']} vs {results2['label']}\n" +
                        f"Correlation: {cross_results['correlation']:.4f}",
                        fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(f'{output_dir}/spreads_scatter.png', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Saved: {output_dir}/spreads_scatter.png")

    def generate_report(self, results1, results2, cross_results, output_dir='output'):
        """
        Generate a comprehensive text report of the analysis.

        Args:
            results1: Analysis results for first pair
            results2: Analysis results for second pair
            cross_results: Cross-analysis results
            output_dir: Directory to save report
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        report_path = f'{output_dir}/analysis_report.txt'

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("FUTURES SPREAD DYNAMICS ANALYSIS\n")
            f.write("Dafu Zhu - Student ID: 12504076\n")
            f.write(f"Analysis Period: {START_DATE} to {END_DATE}\n")
            f.write("=" * 80 + "\n\n")

            # Pair 1 Analysis
            if results1:
                f.write(f"\n{'='*80}\n")
                f.write(f"PAIR 1: {results1['label']}\n")
                f.write(f"{'='*80}\n\n")

                f.write("Basic Statistics:\n")
                f.write(f"  Mean:     {results1['stats']['mean']:.6f}\n")
                f.write(f"  Median:   {results1['stats']['median']:.6f}\n")
                f.write(f"  Std Dev:  {results1['stats']['std']:.6f}\n")
                f.write(f"  Min:      {results1['stats']['min']:.6f}\n")
                f.write(f"  Max:      {results1['stats']['max']:.6f}\n\n")

                f.write("Quantiles:\n")
                for q, val in results1['stats']['quantiles'].items():
                    f.write(f"  {q*100:5.1f}%: {val:.6f}\n")

                f.write("\n" + "-"*80 + "\n")
                f.write("Deviation Analysis (from N-day Rolling Average):\n")
                f.write("-"*80 + "\n")

                for N in ROLLING_WINDOWS:
                    dev_key = f'd_{N}'
                    if dev_key in results1['deviations']:
                        f.write(f"\n{N}-Day Rolling Average Deviation:\n")
                        f.write(f"  Median: {results1['deviations'][dev_key]['median']:.6f}\n")
                        f.write(f"  Std Dev: {results1['deviations'][dev_key]['std']:.6f}\n")
                        f.write("  Quantiles:\n")
                        for q, val in results1['deviations'][dev_key]['quantiles'].items():
                            f.write(f"    {q*100:5.1f}%: {val:.6f}\n")

            # Pair 2 Analysis
            if results2:
                f.write(f"\n\n{'='*80}\n")
                f.write(f"PAIR 2: {results2['label']}\n")
                f.write(f"{'='*80}\n\n")

                f.write("Basic Statistics:\n")
                f.write(f"  Mean:     {results2['stats']['mean']:.6f}\n")
                f.write(f"  Median:   {results2['stats']['median']:.6f}\n")
                f.write(f"  Std Dev:  {results2['stats']['std']:.6f}\n")
                f.write(f"  Min:      {results2['stats']['min']:.6f}\n")
                f.write(f"  Max:      {results2['stats']['max']:.6f}\n\n")

                f.write("Quantiles:\n")
                for q, val in results2['stats']['quantiles'].items():
                    f.write(f"  {q*100:5.1f}%: {val:.6f}\n")

                f.write("\n" + "-"*80 + "\n")
                f.write("Deviation Analysis (from N-day Rolling Average):\n")
                f.write("-"*80 + "\n")

                for N in ROLLING_WINDOWS:
                    dev_key = f'd_{N}'
                    if dev_key in results2['deviations']:
                        f.write(f"\n{N}-Day Rolling Average Deviation:\n")
                        f.write(f"  Median: {results2['deviations'][dev_key]['median']:.6f}\n")
                        f.write(f"  Std Dev: {results2['deviations'][dev_key]['std']:.6f}\n")
                        f.write("  Quantiles:\n")
                        for q, val in results2['deviations'][dev_key]['quantiles'].items():
                            f.write(f"    {q*100:5.1f}%: {val:.6f}\n")

            # Cross-pair analysis
            if cross_results:
                f.write(f"\n\n{'='*80}\n")
                f.write("CROSS-SPREAD ANALYSIS\n")
                f.write(f"{'='*80}\n\n")

                f.write(f"Correlation between spreads: {cross_results['correlation']:.6f}\n\n")

                f.write("Correlations between deviation values:\n")
                for dev_key, corr in cross_results['d_correlations'].items():
                    N = dev_key.split('_')[1]
                    f.write(f"  {N}-day deviations: {corr:.6f}\n")

            f.write("\n" + "="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")

        print(f"\nSaved: {report_path}")

    def close(self):
        """Close WRDS connection."""
        self.db.close()
        print("\nWRDS connection closed.")


def main():
    """Main execution function."""
    print("="*80)
    print("FUTURES SPREAD DYNAMICS ANALYSIS")
    print("Student: Dafu Zhu (12504076)")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print("="*80)

    # Initialize analyzer
    analyzer = FuturesSpreadAnalyzer(WRDS_USERNAME, WRDS_PASSWORD)

    try:
        # Pair 1: CL versus HO
        print("\n" + "="*80)
        print("ANALYZING PAIR 1: CL (Crude Oil) versus HO (Heating Oil)")
        print("="*80)

        cl_data = analyzer.download_futures_data('CL', START_DATE, END_DATE)
        ho_data = analyzer.download_futures_data('HO', START_DATE, END_DATE)

        # Identify top contracts (by data points)
        cl_contracts = analyzer.identify_top_contracts(cl_data, n_contracts=2)
        ho_contracts = analyzer.identify_top_contracts(ho_data, n_contracts=2)

        # Prepare contract data
        print("\nPreparing CL contract data...")
        cl_front = analyzer.prepare_contract_data(cl_data, cl_contracts[0]) if len(cl_contracts) > 0 else None
        cl_second = analyzer.prepare_contract_data(cl_data, cl_contracts[1]) if len(cl_contracts) > 1 else None

        print("\nPreparing HO contract data...")
        ho_front = analyzer.prepare_contract_data(ho_data, ho_contracts[0]) if len(ho_contracts) > 0 else None
        ho_second = analyzer.prepare_contract_data(ho_data, ho_contracts[1]) if len(ho_contracts) > 1 else None

        # Calculate calendar spreads: second month - front month
        print("\nCalculating calendar spreads...")
        cl_spread = analyzer.calculate_calendar_spread(cl_second, cl_front)
        ho_spread = analyzer.calculate_calendar_spread(ho_second, ho_front)

        if cl_spread is not None:
            print(f"CL calendar spread: {cl_spread.notna().sum()} non-null data points")
        if ho_spread is not None:
            print(f"HO calendar spread: {ho_spread.notna().sum()} non-null data points")

        # Analyze CL spread (s1 for pair 1)
        print("\nAnalyzing CL spread dynamics...")
        results_cl = analyzer.analyze_spread_dynamics(cl_spread, 'CL Calendar Spread')

        # Pair 2: YM versus RTY
        print("\n" + "="*80)
        print("ANALYZING PAIR 2: YM (Dow Mini) versus RTY (Russell 2000 Mini)")
        print("="*80)

        ym_data = analyzer.download_futures_data('YM', START_DATE, END_DATE)
        rty_data = analyzer.download_futures_data('RTY', START_DATE, END_DATE)

        # Identify top contracts
        ym_contracts = analyzer.identify_top_contracts(ym_data, n_contracts=2)
        rty_contracts = analyzer.identify_top_contracts(rty_data, n_contracts=2)

        # Prepare contract data
        print("\nPreparing YM contract data...")
        ym_front = analyzer.prepare_contract_data(ym_data, ym_contracts[0]) if len(ym_contracts) > 0 else None
        ym_second = analyzer.prepare_contract_data(ym_data, ym_contracts[1]) if len(ym_contracts) > 1 else None

        print("\nPreparing RTY contract data...")
        rty_front = analyzer.prepare_contract_data(rty_data, rty_contracts[0]) if len(rty_contracts) > 0 else None
        rty_second = analyzer.prepare_contract_data(rty_data, rty_contracts[1]) if len(rty_contracts) > 1 else None

        # Calculate calendar spreads: second month - front month
        print("\nCalculating calendar spreads...")
        ym_spread = analyzer.calculate_calendar_spread(ym_second, ym_front)
        rty_spread = analyzer.calculate_calendar_spread(rty_second, rty_front)

        if ym_spread is not None:
            print(f"YM calendar spread: {ym_spread.notna().sum()} non-null data points")
        if rty_spread is not None:
            print(f"RTY calendar spread: {rty_spread.notna().sum()} non-null data points")

        # Analyze YM spread (s1 for pair 2)
        print("\nAnalyzing YM spread dynamics...")
        results_ym = analyzer.analyze_spread_dynamics(ym_spread, 'YM Calendar Spread')

        # Cross-spread analysis
        print("\n" + "="*80)
        print("CROSS-SPREAD ANALYSIS")
        print("="*80)

        cross_results = analyzer.analyze_cross_spread_dynamics(
            cl_spread, ym_spread, 'CL Spread', 'YM Spread'
        )

        print(f"\nCorrelation between CL and YM spreads: {cross_results['correlation']:.4f}")

        # Generate visualizations
        print("\n" + "="*80)
        print("GENERATING VISUALIZATIONS")
        print("="*80)
        analyzer.create_visualizations(results_cl, results_ym, cross_results)

        # Generate report
        print("\n" + "="*80)
        print("GENERATING REPORT")
        print("="*80)
        analyzer.generate_report(results_cl, results_ym, cross_results)

        print("\n" + "="*80)
        print("ANALYSIS COMPLETE!")
        print("="*80)
        print("\nCheck the 'output' directory for:")
        print("  - analysis_report.txt (comprehensive statistics)")
        print("  - spreads_timeseries.png (time series plots)")
        print("  - spreads_distribution.png (distribution histograms)")
        print("  - spread1_deviations.png (CL deviation analysis)")
        print("  - spread2_deviations.png (YM deviation analysis)")
        print("  - spreads_scatter.png (correlation scatter plot)")

    except Exception as e:
        print(f"\nError during analysis: {e}")
        import traceback
        traceback.print_exc()

    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
