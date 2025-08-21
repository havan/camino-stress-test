#!/usr/bin/env python3
"""
Camino Stress Testing Tool

Fire and forget transactions to stress test the chains performance. Please note that
the RPC API endpoint might be rate-limited. Use your own node if possible.
"""

import asyncio
import argparse
import os
import time
import statistics
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging

# Third-party imports
try:
    from web3 import AsyncWeb3
    from eth_account import Account
    from dotenv import load_dotenv
    from rich.console import Console
    from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("📦 Install with: pip install web3 python-dotenv rich eth-account")
    exit(1)


@dataclass
class Config:
    """Configuration settings for the stress tester."""
    rpc_url: str = "https://columbus.camino.network/ext/bc/C/rpc"
    chain_id: int = 501
    gas_price: int = 200_000_000_000  # 200 gwei in wei
    private_key: str = ""
    to_address: str = ""
    max_concurrent: int = 50
    gas_limit: int = 21000


@dataclass
class TransactionMetrics:
    """Metrics for individual transaction submissions."""
    start_time: float
    end_time: float
    success: bool
    error: Optional[str] = None

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time


@dataclass
class StressTestResults:
    """Comprehensive results from stress testing session."""
    total_transactions: int = 0
    successful_transactions: int = 0
    failed_transactions: int = 0
    total_duration: float = 0.0
    transaction_times: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_transactions == 0:
            return 0.0
        return (self.successful_transactions / self.total_transactions) * 100

    @property
    def average_tps(self) -> float:
        if self.total_duration == 0:
            return 0.0
        return self.total_transactions / self.total_duration

    @property
    def average_transaction_time(self) -> float:
        if not self.transaction_times:
            return 0.0
        return statistics.mean(self.transaction_times)

    @property
    def min_transaction_time(self) -> float:
        if not self.transaction_times:
            return 0.0
        return min(self.transaction_times)

    @property
    def max_transaction_time(self) -> float:
        if not self.transaction_times:
            return 0.0
        return max(self.transaction_times)


class BlockchainStressTester:
    """High-performance blockchain stress testing class."""

    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        self.w3: Optional[AsyncWeb3] = None
        self.account = None
        self.current_nonce: int = 0
        self.nonce_lock = asyncio.Lock()

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('stress_test.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        """Initialize the stress tester with blockchain connection and account setup."""
        self.console.print(Panel.fit("🚀 Initializing Camino Stress Tester", style="bold blue"))

        # Setup Web3 connection
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.config.rpc_url))

        # Verify connection
        try:
            is_connected = await self.w3.is_connected()
            if not is_connected:
                raise ConnectionError("Failed to connect to blockchain")

            self.console.print("✅ Connected to blockchain", style="green")

            # Setup account
            self.account = Account.from_key(self.config.private_key)
            self.console.print(f"👛 Wallet address: {self.account.address}", style="cyan")

            # Get initial nonce
            self.current_nonce = await self.w3.eth.get_transaction_count(self.account.address)
            self.console.print(f"🔢 Starting nonce: {self.current_nonce}", style="yellow")

            # Check balance
            balance = await self.w3.eth.get_balance(self.account.address)
            balance_eth = self.w3.from_wei(balance, 'ether')
            self.console.print(f"💰 Wallet balance: {balance_eth:.6f} ETH", style="green")

        except Exception as e:
            self.console.print(f"❌ Initialization failed: {e}", style="red")
            raise

    async def get_next_nonce(self) -> int:
        """Thread-safe nonce management."""
        async with self.nonce_lock:
            nonce = self.current_nonce
            self.current_nonce += 1
            return nonce

    async def send_transaction_async(self, value_wei: int = 0) -> TransactionMetrics:
        """Send a single transaction asynchronously."""
        start_time = time.time()

        try:
            nonce = await self.get_next_nonce()

            # Build transaction
            transaction = {
                'to': self.config.to_address,
                'value': value_wei,
                'gas': self.config.gas_limit,
                'gasPrice': self.config.gas_price,
                'nonce': nonce,
                'chainId': self.config.chain_id
            }

            # Sign transaction
            signed_txn = self.account.sign_transaction(transaction)

            # Send transaction (fire and forget) - handle different attribute names
            raw_transaction = getattr(signed_txn, 'raw_transaction',
                                      getattr(signed_txn, 'rawTransaction', None))

            if raw_transaction is None:
                raise AttributeError("Could not find raw transaction data")

            await self.w3.eth.send_raw_transaction(raw_transaction)

            end_time = time.time()
            return TransactionMetrics(start_time, end_time, True)

        except Exception as e:
            end_time = time.time()
            self.logger.error(f"Transaction failed: {e}")
            return TransactionMetrics(start_time, end_time, False, str(e))

    async def stress_test_batch(self, batch_size: int, semaphore: asyncio.Semaphore) -> List[TransactionMetrics]:
        """Process a batch of transactions with concurrency control."""
        async with semaphore:
            tasks = [self.send_transaction_async() for _ in range(batch_size)]
            return await asyncio.gather(*tasks)

    async def run_stress_test(self, total_transactions: int) -> StressTestResults:
        """Execute the main stress testing routine."""
        self.console.print(Panel.fit(f"🔥 Starting Stress Test - {total_transactions:,} Transactions", style="bold red"))

        results = StressTestResults()
        results.total_transactions = total_transactions

        # Calculate batching strategy
        batch_size = min(self.config.max_concurrent, total_transactions)
        num_batches = (total_transactions + batch_size - 1) // batch_size

        # Semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        start_time = time.time()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            console=self.console,
            refresh_per_second=4
        ) as progress:

            task = progress.add_task(
                "🚀 Submitting transactions...",
                total=total_transactions
            )

            # Process transactions in batches
            for batch_num in range(num_batches):
                current_batch_size = min(batch_size, total_transactions - batch_num * batch_size)

                # Execute batch
                batch_results = await self.stress_test_batch(current_batch_size, semaphore)

                # Process results
                for metric in batch_results:
                    if metric.success:
                        results.successful_transactions += 1
                    else:
                        results.failed_transactions += 1
                        if metric.error:
                            results.errors.append(metric.error)

                    results.transaction_times.append(metric.duration)

                # Update progress
                progress.update(task, advance=current_batch_size)

                # Calculate current TPS for display
                elapsed = time.time() - start_time
                current_tps = (batch_num + 1) * batch_size / elapsed if elapsed > 0 else 0
                progress.update(task, description=f"🚀 Submitting transactions... (TPS: {current_tps:.1f})")

        results.total_duration = time.time() - start_time
        return results

    def display_results(self, results: StressTestResults) -> None:
        """Display comprehensive test results."""
        self.console.print("\n" + "="*60)
        self.console.print(Panel.fit("📊 Stress Test Results", style="bold green"))

        # Create results table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")
        table.add_column("Unit", style="yellow")

        # Add metrics to table
        table.add_row("🎯 Total Transactions", f"{results.total_transactions:,}", "txns")
        table.add_row("✅ Successful", f"{results.successful_transactions:,}", "txns")
        table.add_row("❌ Failed", f"{results.failed_transactions:,}", "txns")
        table.add_row("📈 Success Rate", f"{results.success_rate:.2f}", "%")
        table.add_row("⏱️  Total Duration", f"{results.total_duration:.3f}", "seconds")
        table.add_row("🚀 Average TPS", f"{results.average_tps:.2f}", "txns/sec")
        table.add_row("⚡ Avg Tx Time", f"{results.average_transaction_time*1000:.2f}", "ms")
        table.add_row("🏃 Min Tx Time", f"{results.min_transaction_time*1000:.2f}", "ms")
        table.add_row("🐌 Max Tx Time", f"{results.max_transaction_time*1000:.2f}", "ms")

        self.console.print(table)

        # Display errors if any
        if results.errors:
            self.console.print(f"\n❌ Errors encountered ({len(results.errors)}):", style="red")
            error_counts = {}
            for error in results.errors:
                error_counts[error] = error_counts.get(error, 0) + 1

            for error, count in error_counts.items():
                self.console.print(f"  • {error}: {count} times", style="red")

    async def cleanup(self) -> None:
        """Clean up resources."""
        # Web3 AsyncHTTPProvider handles its own cleanup automatically
        pass


def load_configuration(config_path: str) -> Config:
    """Load configuration from environment file."""
    console = Console()

    if not os.path.exists(config_path):
        console.print(f"⚠️  Config file not found: {config_path}", style="yellow")
        console.print("📝 Creating default config file...", style="blue")

        # Create default config file
        default_config = """# Blockchain Stress Testing Configuration
RPC_URL=https://columbus.camino.network/ext/bc/C/rpc
CHAIN_ID=501
PRIVATE_KEY=your_private_key_here
TO_ADDRESS=0x0000000000000000000000000000000000000000
GAS_PRICE=200000000000
MAX_CONCURRENT=50
GAS_LIMIT=21000
"""
        with open(config_path, 'w') as f:
            f.write(default_config)

        console.print(f"✅ Created {config_path}. Please edit it with your settings.", style="green")
        exit(1)

    # Load environment variables
    load_dotenv(config_path)

    config = Config(
        rpc_url=os.getenv('RPC_URL', Config.rpc_url),
        chain_id=int(os.getenv('CHAIN_ID', Config.chain_id)),
        private_key=os.getenv('PRIVATE_KEY', ''),
        to_address=os.getenv('TO_ADDRESS', ''),
        gas_price=int(os.getenv('GAS_PRICE', Config.gas_price)),
        max_concurrent=int(os.getenv('MAX_CONCURRENT', Config.max_concurrent)),
        gas_limit=int(os.getenv('GAS_LIMIT', Config.gas_limit))
    )

    # Validate required fields
    if not config.private_key or config.private_key == 'your_private_key_here':
        console.print("❌ PRIVATE_KEY not set in config file", style="red")
        exit(1)

    if not config.to_address or config.to_address == '0x0000000000000000000000000000000000000000':
        console.print("❌ TO_ADDRESS not set in config file", style="red")
        exit(1)

    return config


async def main():
    """Main async function that orchestrates the stress testing."""
    parser = argparse.ArgumentParser(
        description="🔥 Camino Stress Stress Testing Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python stress_test.py --count 1000 --config .env
  python stress_test.py --count 5000 --config mainnet.config
        """
    )

    parser.add_argument(
        '--config',
        default='.config',
        help='Path to configuration file (default: .config)'
    )

    parser.add_argument(
        '--count',
        type=int,
        default=1000,
        help='Number of transactions to send (default: 1000)'
    )

    args = parser.parse_args()

    console = Console()
    console.print(Panel.fit("🔥 Camino Stress Testing Tool", style="bold red"))
    console.print(f"📁 Config file: {args.config}")
    console.print(f"📊 Transaction count: {args.count:,}")

    try:
        # Load configuration
        config = load_configuration(args.config)

        # Initialize and run stress tester
        tester = BlockchainStressTester(config)

        try:
            await tester.initialize()
            results = await tester.run_stress_test(args.count)
            tester.display_results(results)

        finally:
            await tester.cleanup()

    except KeyboardInterrupt:
        console.print("\n🛑 Stress test interrupted by user", style="yellow")
    except Exception as e:
        console.print(f"\n💥 Stress test failed: {e}", style="red")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
