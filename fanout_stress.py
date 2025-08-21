#!/usr/bin/env python3
"""
Fan-out Multi-Wallet Blockchain Stress Testing Tool
Distributes funds to multiple wallets and runs concurrent stress tests for maximum TPS.
Enhanced with proper multiprocessing and session management.
"""

import asyncio
import argparse
import os
import time
import json
import statistics
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
import logging
from pathlib import Path
from multiprocessing import Pool, Process, Queue, cpu_count
import multiprocessing
import signal
import sys

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
    from rich.live import Live
    from rich.layout import Layout
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("📦 Install with: pip install web3 python-dotenv rich eth-account")
    exit(1)


@dataclass
class Config:
    """Configuration settings for the fan-out stress tester."""
    rpc_url: str = "https://columbus.camino.network/ext/bc/C/rpc"
    chain_id: int = 501
    gas_price: int = 200_000_000_000  # 200 gwei in wei
    private_key: str = ""
    to_address: str = ""
    max_concurrent: int = 50
    gas_limit: int = 21000


@dataclass
class WalletInfo:
    """Information about a generated wallet."""
    private_key: str
    address: str
    initial_balance: int = 0
    final_balance: int = 0
    distribution_tx_hash: str = ""
    recovery_tx_hash: str = ""
    last_nonce_used: int = 0  # Track the last nonce used in stress test


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
    wallet_address: str = ""
    total_transactions: int = 0
    successful_transactions: int = 0
    failed_transactions: int = 0
    total_duration: float = 0.0
    transaction_times: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    last_nonce_used: int = 0  # Track the last nonce used

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


@dataclass
class FanOutResults:
    """Results from the entire fan-out stress testing session."""
    total_wallets: int = 0
    total_transactions: int = 0
    successful_transactions: int = 0
    failed_transactions: int = 0
    total_duration: float = 0.0  # Duration of stress test phase only
    combined_tps: float = 0.0
    wallet_results: List[StressTestResults] = field(default_factory=list)
    wallets_info: List[WalletInfo] = field(default_factory=list)
    distribution_duration: float = 0.0  # Time for fund distribution
    recovery_duration: float = 0.0  # Time for fund recovery
    overall_duration: float = 0.0  # Total time including all phases


def run_wallet_stress_test_process(args: Tuple[Config, str, int, int]) -> StressTestResults:
    """
    Run stress test for a single wallet in a separate process.
    This function runs in its own process to avoid GIL limitations.
    """
    config, wallet_private_key, wallet_id, transactions = args

    # Create new event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        result = loop.run_until_complete(
            run_wallet_stress_test_async(config, wallet_private_key, wallet_id, transactions)
        )
        return result
    finally:
        loop.close()


async def run_wallet_stress_test_async(config: Config, wallet_private_key: str, wallet_id: int, transactions: int) -> StressTestResults:
    """
    Async function to run stress test for a single wallet.
    """
    # Setup logging for this wallet
    logger = logging.getLogger(f'wallet_{wallet_id}')

    # Initialize Web3 connection for this process
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(config.rpc_url))

    try:
        # Setup account
        account = Account.from_key(wallet_private_key)

        # Get initial nonce
        current_nonce = await w3.eth.get_transaction_count(account.address)
        initial_nonce = current_nonce

        results = StressTestResults()
        results.wallet_address = account.address
        results.total_transactions = transactions

        # Prepare batch of transactions
        batch_size = min(config.max_concurrent, transactions)
        num_batches = (transactions + batch_size - 1) // batch_size

        start_time = time.time()

        for batch_num in range(num_batches):
            current_batch_size = min(batch_size, transactions - batch_num * batch_size)

            # Create tasks for this batch
            tasks = []
            for _ in range(current_batch_size):
                transaction = {
                    'to': config.to_address,
                    'value': 0,
                    'gas': config.gas_limit,
                    'gasPrice': config.gas_price,
                    'nonce': current_nonce,
                    'chainId': config.chain_id
                }
                current_nonce += 1

                # Sign transaction
                signed_txn = account.sign_transaction(transaction)
                raw_transaction = getattr(signed_txn, 'raw_transaction',
                                          getattr(signed_txn, 'rawTransaction', None))

                # Create send task
                task = w3.eth.send_raw_transaction(raw_transaction)
                tasks.append(task)

            # Execute batch with gather
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    results.failed_transactions += 1
                    results.errors.append(str(result))
                else:
                    results.successful_transactions += 1

        results.total_duration = time.time() - start_time
        results.last_nonce_used = current_nonce - 1

        return results

    finally:
        # Properly close the Web3 connection
        if hasattr(w3.provider, '_session'):
            await w3.provider._session.close()
        elif hasattr(w3.provider, 'session'):
            await w3.provider.session.close()


class FanOutStressTester:
    """Fan-out stress tester that manages multiple wallets with multiprocessing."""

    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        self.w3: Optional[AsyncWeb3] = None
        self.master_account = None
        self.wallets: List[WalletInfo] = []
        self.artifacts_dir = Path("stress_artifacts")

        # Setup logging
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

    def setup_logging(self):
        """Setup comprehensive logging."""
        self.artifacts_dir.mkdir(exist_ok=True)

        log_file = self.artifacts_dir / f"fanout_stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # Configure multiprocessing-aware logging
        multiprocessing.log_to_stderr()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    async def initialize(self) -> None:
        """Initialize the fan-out stress tester."""
        self.console.print(Panel.fit("🌟 Initializing Fan-out Stress Tester", style="bold blue"))

        # Setup Web3 connection with proper session management
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.config.rpc_url))

        # Verify connection
        is_connected = await self.w3.is_connected()
        if not is_connected:
            raise ConnectionError("Failed to connect to blockchain")

        self.console.print("✅ Connected to blockchain", style="green")

        # Setup master account
        self.master_account = Account.from_key(self.config.private_key)
        self.console.print(f"👛 Master wallet: {self.master_account.address}", style="cyan")

        # Check master balance
        balance = await self.w3.eth.get_balance(self.master_account.address)
        balance_eth = self.w3.from_wei(balance, 'ether')
        self.console.print(f"💰 Master balance: {balance_eth:.6f} ETH", style="green")

    async def cleanup(self) -> None:
        """Cleanup all resources including Web3 sessions."""
        if self.w3:
            try:
                # Try different ways to access and close the session
                if hasattr(self.w3.provider, '_session'):
                    if self.w3.provider._session and not self.w3.provider._session.closed:
                        await self.w3.provider._session.close()
                elif hasattr(self.w3.provider, 'session'):
                    if self.w3.provider.session and not self.w3.provider.session.closed:
                        await self.w3.provider.session.close()
                elif hasattr(self.w3.provider, 'disconnect'):
                    await self.w3.provider.disconnect()
            except Exception as e:
                self.logger.warning(f"Error during cleanup: {e}")

    def generate_wallets(self, num_wallets: int) -> List[WalletInfo]:
        """Generate wallets by incrementing the master private key."""
        self.console.print(f"🔧 Generating {num_wallets} wallets...", style="yellow")

        wallets = []
        master_key_int = int(self.config.private_key, 16)

        for i in range(1, num_wallets + 1):
            # Increment private key
            new_key_int = master_key_int + i
            new_private_key = f"{new_key_int:064x}"

            # Create account
            account = Account.from_key(new_private_key)

            wallet_info = WalletInfo(
                private_key=new_private_key,
                address=account.address
            )
            wallets.append(wallet_info)

            self.console.print(f"  🔑 Wallet {i}: {account.address}", style="dim cyan")

        return wallets

    async def distribute_funds(self, wallets: List[WalletInfo], transactions_per_wallet: int) -> None:
        """Distribute funds from master wallet to generated wallets."""
        self.console.print("💸 Distributing funds to wallets...", style="yellow")

        # Calculate required amount per wallet (with extra buffer for safety)
        gas_cost_per_tx = self.config.gas_limit * self.config.gas_price
        required_per_wallet = gas_cost_per_tx * (transactions_per_wallet + 10)  # +10 for extra safety margin

        # Get master balance
        master_balance = await self.w3.eth.get_balance(self.master_account.address)

        # Calculate distribution amount
        distribution_gas_cost = len(wallets) * gas_cost_per_tx
        recovery_gas_cost = len(wallets) * gas_cost_per_tx
        total_required = (required_per_wallet * len(wallets)) + distribution_gas_cost + recovery_gas_cost

        if master_balance < total_required:
            required_eth = self.w3.from_wei(total_required, 'ether')
            available_eth = self.w3.from_wei(master_balance, 'ether')
            raise ValueError(
                f"Insufficient balance. Required: {required_eth:.6f} ETH, Available: {available_eth:.6f} ETH")

        # Get master nonce
        master_nonce = await self.w3.eth.get_transaction_count(self.master_account.address)

        # Distribute funds
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Distributing funds...", total=len(wallets))

            for i, wallet in enumerate(wallets):
                try:
                    # Build distribution transaction
                    transaction = {
                        'to': wallet.address,
                        'value': required_per_wallet,
                        'gas': self.config.gas_limit,
                        'gasPrice': self.config.gas_price,
                        'nonce': master_nonce + i,
                        'chainId': self.config.chain_id
                    }

                    # Sign and send
                    signed_txn = self.master_account.sign_transaction(transaction)
                    raw_transaction = getattr(signed_txn, 'raw_transaction',
                                              getattr(signed_txn, 'rawTransaction', None))

                    tx_hash = await self.w3.eth.send_raw_transaction(raw_transaction)
                    wallet.distribution_tx_hash = tx_hash.hex()
                    wallet.initial_balance = required_per_wallet

                    progress.update(task, advance=1)

                except Exception as e:
                    self.logger.error(f"Failed to distribute to wallet {wallet.address}: {e}")
                    raise

        self.console.print("✅ Fund distribution completed", style="green")

    def run_multiprocess_stress_tests(self, wallets: List[WalletInfo], transactions_per_wallet: int) -> List[StressTestResults]:
        """Run stress tests using multiprocessing for true parallelization."""
        self.console.print(
            f"🚀 Starting multiprocess stress tests ({len(wallets)} wallets, {transactions_per_wallet} txns each)...",
            style="red"
        )

        # Prepare arguments for each process
        process_args = [
            (self.config, wallet.private_key, i + 1, transactions_per_wallet)
            for i, wallet in enumerate(wallets)
        ]

        # Determine number of processes to use
        num_processes = min(len(wallets), cpu_count())
        self.console.print(f"📊 Using {num_processes} parallel processes", style="cyan")

        start_time = time.time()

        # Run stress tests in parallel processes
        with Pool(processes=num_processes) as pool:
            results = pool.map(run_wallet_stress_test_process, process_args)

        # Update wallet info with last nonce used
        for i, result in enumerate(results):
            wallets[i].last_nonce_used = result.last_nonce_used

        total_duration = time.time() - start_time
        self.console.print(f"✅ All stress tests completed in {total_duration:.3f} seconds", style="green")

        return results

    async def wait_for_transactions_to_settle(self, wait_time: int = 10) -> None:
        """Wait for pending transactions to be mined."""
        self.console.print(f"⏳ Waiting {wait_time} seconds for transactions to settle...", style="yellow")
        await asyncio.sleep(wait_time)

    async def recover_funds_with_retry(self, wallet: WalletInfo, max_retries: int = 3) -> bool:
        """Recover funds from a wallet with retry logic."""
        for attempt in range(max_retries):
            try:
                # Create temporary account for this wallet
                temp_account = Account.from_key(wallet.private_key)

                # Get current balance
                current_balance = await self.w3.eth.get_balance(wallet.address)

                if current_balance > 0:
                    # Calculate amount to send (leave gas for transaction)
                    gas_cost = self.config.gas_limit * self.config.gas_price

                    # Check if we have enough balance to cover gas
                    if current_balance <= gas_cost:
                        self.logger.warning(
                            f"Wallet {wallet.address} has insufficient balance for recovery: {current_balance} wei")
                        wallet.final_balance = current_balance
                        return False

                    amount_to_send = current_balance - gas_cost

                    # Use the last nonce + 1 for recovery (to avoid nonce conflicts)
                    recovery_nonce = wallet.last_nonce_used + 1

                    # Build recovery transaction
                    transaction = {
                        'to': self.master_account.address,
                        'value': amount_to_send,
                        'gas': self.config.gas_limit,
                        'gasPrice': self.config.gas_price,
                        'nonce': recovery_nonce,
                        'chainId': self.config.chain_id
                    }

                    # Sign and send
                    signed_txn = temp_account.sign_transaction(transaction)
                    raw_transaction = getattr(signed_txn, 'raw_transaction',
                                              getattr(signed_txn, 'rawTransaction', None))

                    tx_hash = await self.w3.eth.send_raw_transaction(raw_transaction)
                    wallet.recovery_tx_hash = tx_hash.hex()

                    # Update final balance after successful recovery
                    await asyncio.sleep(2)  # Give it a moment to process
                    wallet.final_balance = await self.w3.eth.get_balance(wallet.address)
                    return True
                else:
                    wallet.final_balance = 0
                    return True

            except Exception as e:
                error_str = str(e)
                if "replacement transaction underpriced" in error_str and attempt < max_retries - 1:
                    # Increment nonce and retry
                    wallet.last_nonce_used += 1
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                elif "insufficient funds" in error_str:
                    # Not enough funds, skip this wallet
                    self.logger.warning(f"Wallet {wallet.address} has insufficient funds for recovery")
                    wallet.final_balance = await self.w3.eth.get_balance(wallet.address)
                    return False
                else:
                    self.logger.error(f"Failed to recover from wallet {wallet.address} (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        wallet.final_balance = await self.w3.eth.get_balance(wallet.address)
                        return False

        return False

    async def recover_funds(self, wallets: List[WalletInfo]) -> None:
        """Recover remaining funds from wallets back to master."""
        self.console.print("💰 Recovering funds from wallets...", style="yellow")

        # Wait for transactions to settle first
        await self.wait_for_transactions_to_settle(10)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Recovering funds...", total=len(wallets))

            for wallet in wallets:
                success = await self.recover_funds_with_retry(wallet)
                if success:
                    self.logger.info(f"Successfully recovered funds from {wallet.address}")
                progress.update(task, advance=1)

        self.console.print("✅ Fund recovery completed", style="green")

    def save_artifacts(self, results: FanOutResults) -> None:
        """Save all artifacts to files."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save detailed results
        results_file = self.artifacts_dir / f"fanout_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            # Convert dataclasses to dict for JSON serialization
            results_dict = asdict(results)
            json.dump(results_dict, f, indent=2, default=str)

        # Save wallet information
        wallets_file = self.artifacts_dir / f"wallets_info_{timestamp}.json"
        with open(wallets_file, 'w') as f:
            wallets_dict = [asdict(wallet) for wallet in results.wallets_info]
            json.dump(wallets_dict, f, indent=2)

        # Save summary report
        summary_file = self.artifacts_dir / f"summary_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write(f"Fan-out Stress Test Summary - {timestamp}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total Wallets: {results.total_wallets}\n")
            f.write(f"Total Transactions: {results.total_transactions}\n")
            f.write(f"Successful Transactions: {results.successful_transactions}\n")
            f.write(f"Failed Transactions: {results.failed_transactions}\n")
            if results.total_transactions > 0:
                f.write(f"Success Rate: {(results.successful_transactions/results.total_transactions*100):.2f}%\n")
            f.write(f"Stress Test Duration: {results.total_duration:.3f} seconds\n")
            f.write(f"Combined TPS: {results.combined_tps:.2f}\n\n")

            f.write("Timing Breakdown:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Fund Distribution: {results.distribution_duration:.2f} seconds\n")
            f.write(f"Stress Testing: {results.total_duration:.2f} seconds\n")
            f.write(f"Fund Recovery: {results.recovery_duration:.2f} seconds\n")
            f.write(f"Total Runtime: {results.overall_duration:.2f} seconds\n\n")

            f.write("Note: TPS is calculated using only the stress test phase duration,\n")
            f.write("excluding fund distribution and recovery overhead.\n")
            f.write("Also note that we are not waiting for transactions to be mined.\n\n")

            f.write("Individual Wallet Results:\n")
            f.write("-" * 30 + "\n")
            for i, wallet_result in enumerate(results.wallet_results):
                f.write(f"Wallet {i+1} ({wallet_result.wallet_address}):\n")
                f.write(f"  TPS: {wallet_result.average_tps:.2f}\n")
                f.write(f"  Success Rate: {wallet_result.success_rate:.2f}%\n")
                f.write(f"  Duration: {wallet_result.total_duration:.3f}s\n\n")

        self.console.print(f"📄 Artifacts saved to {self.artifacts_dir}/", style="green")

    def display_results(self, results: FanOutResults) -> None:
        """Display comprehensive fan-out test results."""
        self.console.print("\n" + "="*80)
        self.console.print(Panel.fit("🎯 Fan-out Stress Test Results", style="bold green"))

        # Summary table
        summary_table = Table(show_header=True, header_style="bold magenta", title="📊 Overall Summary")
        summary_table.add_column("Metric", style="cyan", no_wrap=True)
        summary_table.add_column("Value", style="green")
        summary_table.add_column("Unit", style="yellow")

        summary_table.add_row("🔱 Total Wallets", f"{results.total_wallets}", "wallets")
        summary_table.add_row("🎯 Total Transactions", f"{results.total_transactions:,}", "txns")
        summary_table.add_row("✅ Successful", f"{results.successful_transactions:,}", "txns")
        summary_table.add_row("❌ Failed", f"{results.failed_transactions:,}", "txns")

        if results.total_transactions > 0:
            success_rate = (results.successful_transactions/results.total_transactions*100)
            summary_table.add_row("📈 Success Rate", f"{success_rate:.2f}", "%")
        else:
            summary_table.add_row("📈 Success Rate", "0.00", "%")

        summary_table.add_row("⏱️ Stress Test Duration", f"{results.total_duration:.3f}", "seconds")
        summary_table.add_row("🚀 Combined TPS", f"{results.combined_tps:.2f}", "txns/sec")

        self.console.print(summary_table)

        # Timing breakdown table
        timing_table = Table(show_header=True, header_style="bold yellow", title="⏰ Timing Breakdown")
        timing_table.add_column("Phase", style="cyan")
        timing_table.add_column("Duration", style="green")
        timing_table.add_column("Percentage", style="yellow")

        if results.overall_duration > 0:
            dist_pct = (results.distribution_duration / results.overall_duration) * 100
            stress_pct = (results.total_duration / results.overall_duration) * 100
            recovery_pct = (results.recovery_duration / results.overall_duration) * 100

            timing_table.add_row("Fund Distribution", f"{results.distribution_duration:.2f}s", f"{dist_pct:.1f}%")
            timing_table.add_row("Stress Testing", f"{results.total_duration:.2f}s", f"{stress_pct:.1f}%")
            timing_table.add_row("Fund Recovery", f"{results.recovery_duration:.2f}s", f"{recovery_pct:.1f}%")
            timing_table.add_row("─" * 15, "─" * 10, "─" * 10)
            timing_table.add_row("Total Runtime", f"{results.overall_duration:.2f}s", "100.0%")

        self.console.print(timing_table)

        # Individual wallet results
        if results.wallet_results:
            wallet_table = Table(show_header=True, header_style="bold blue", title="👛 Individual Wallet Performance")
            wallet_table.add_column("Wallet", style="cyan")
            wallet_table.add_column("Address", style="dim")
            wallet_table.add_column("TPS", style="green")
            wallet_table.add_column("Success Rate", style="yellow")
            wallet_table.add_column("Duration", style="magenta")

            for i, result in enumerate(results.wallet_results):
                wallet_table.add_row(
                    f"Wallet {i+1}",
                    result.wallet_address[:10] + "...",
                    f"{result.average_tps:.1f}",
                    f"{result.success_rate:.1f}%",
                    f"{result.total_duration:.2f}s"
                )

            self.console.print(wallet_table)

        # Note about TPS calculation
        self.console.print("\n📝 [dim]Note: TPS is calculated using only the stress test phase duration,[/dim]")
        self.console.print("   [dim]excluding fund distribution and recovery overhead.[/dim]")
        self.console.print("   [dim]Also note that we are not waiting for transactions to be mined.[/dim]")

    async def run_fanout_stress_test(self, num_wallets: int, transactions_per_wallet: int) -> FanOutResults:
        """Execute the complete fan-out stress testing process."""
        overall_start_time = time.time()

        try:
            # Generate wallets
            self.wallets = self.generate_wallets(num_wallets)

            # Distribute funds
            distribution_start = time.time()
            await self.distribute_funds(self.wallets, transactions_per_wallet)
            distribution_duration = time.time() - distribution_start

            # Wait a bit for transactions to be mined
            self.console.print("⏳ Waiting for distribution transactions to be mined...", style="yellow")
            await asyncio.sleep(5)

            # Run stress tests using multiprocessing (THIS is what we measure for TPS)
            stress_test_start = time.time()
            wallet_results = self.run_multiprocess_stress_tests(self.wallets, transactions_per_wallet)
            stress_test_duration = time.time() - stress_test_start

            # Recover funds
            recovery_start = time.time()
            await self.recover_funds(self.wallets)
            recovery_duration = time.time() - recovery_start

            # Calculate overall duration
            overall_duration = time.time() - overall_start_time

            # Calculate proper TPS (only from stress test phase)
            stress_test_transactions = sum(r.successful_transactions for r in wallet_results)
            combined_tps = stress_test_transactions / stress_test_duration if stress_test_duration > 0 else 0

            # Log timing breakdown
            self.logger.info(f"Timing breakdown:")
            self.logger.info(f"  Distribution: {distribution_duration:.2f}s")
            self.logger.info(f"  Stress Test: {stress_test_duration:.2f}s")
            self.logger.info(f"  Recovery: {recovery_duration:.2f}s")
            self.logger.info(f"  Overall: {overall_duration:.2f}s")

            results = FanOutResults(
                total_wallets=num_wallets,
                total_transactions=sum(r.total_transactions for r in wallet_results),
                successful_transactions=stress_test_transactions,
                failed_transactions=sum(r.failed_transactions for r in wallet_results),
                total_duration=stress_test_duration,  # Use only stress test duration
                combined_tps=combined_tps,
                wallet_results=wallet_results,
                wallets_info=self.wallets,
                distribution_duration=distribution_duration,
                recovery_duration=recovery_duration,
                overall_duration=overall_duration
            )

            return results

        except Exception as e:
            self.logger.error(f"Fan-out stress test failed: {e}")
            # Still try to recover funds
            if self.wallets:
                try:
                    await self.recover_funds(self.wallets)
                except Exception as recovery_error:
                    self.logger.error(f"Fund recovery also failed: {recovery_error}")
            raise
        finally:
            # Always cleanup connections
            await self.cleanup()


def load_configuration(config_path: str) -> Config:
    """Load configuration from environment file."""
    console = Console()

    if not os.path.exists(config_path):
        console.print(f"⚠️  Config file not found: {config_path}", style="yellow")
        console.print("📝 Creating default config file...", style="blue")

        # Create default config file
        default_config = """# Fan-out Blockchain Stress Testing Configuration
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
    """Main async function for fan-out stress testing."""
    parser = argparse.ArgumentParser(
        description="🌟 Fan-out Multi-Wallet Stress Testing Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fanout_stress.py --wallets 5 --count 1000 --config .env
  python fanout_stress.py --wallets 10 --count 500 --config mainnet.config
        """
    )

    parser.add_argument(
        '--config',
        default='.config',
        help='Path to configuration file (default: .config)'
    )

    parser.add_argument(
        '--wallets',
        type=int,
        required=True,
        help='Number of wallets to create and use for stress testing'
    )

    parser.add_argument(
        '--count',
        type=int,
        default=1000,
        help='Number of transactions per wallet (default: 1000)'
    )

    args = parser.parse_args()

    console = Console()
    console.print(Panel.fit("🌟 Fan-out Multi-Wallet Stress Testing Tool", style="bold red"))
    console.print(f"📁 Config file: {args.config}")
    console.print(f"👛 Number of wallets: {args.wallets}")
    console.print(f"📊 Transactions per wallet: {args.count:,}")
    console.print(f"🎯 Total transactions: {args.wallets * args.count:,}")
    console.print(f"💻 CPU cores available: {cpu_count()}")

    try:
        # Load configuration
        config = load_configuration(args.config)

        # Initialize and run fan-out stress tester
        tester = FanOutStressTester(config)

        await tester.initialize()
        results = await tester.run_fanout_stress_test(args.wallets, args.count)

        # Display and save results
        tester.display_results(results)
        tester.save_artifacts(results)

    except KeyboardInterrupt:
        console.print("\n🛑 Fan-out stress test interrupted by user", style="yellow")
    except Exception as e:
        console.print(f"\n💥 Fan-out stress test failed: {e}", style="red")
        raise


if __name__ == "__main__":
    # Set start method for multiprocessing to avoid issues
    multiprocessing.set_start_method('spawn', force=True)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
