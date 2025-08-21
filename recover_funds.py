#!/usr/bin/env python3
"""
Fan-out Funds Recovery Tool
Recovers remaining funds from generated wallets back to the master wallet.
"""

import asyncio
import argparse
import os
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from decimal import Decimal

# Third-party imports
try:
    from web3 import AsyncWeb3
    from eth_account import Account
    from dotenv import load_dotenv
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich.text import Text
except ImportError as e:
    print(f"❌ Missing required dependencies: {e}")
    print("📦 Install with: pip install web3 python-dotenv rich eth-account")
    exit(1)


@dataclass
class Config:
    """Configuration settings for the recovery tool."""
    rpc_url: str = "https://columbus.camino.network/ext/bc/C/rpc"
    chain_id: int = 501
    gas_price: int = 200_000_000_000  # 200 gwei in wei
    private_key: str = ""
    to_address: str = ""  # Not used in recovery, but kept for compatibility
    max_concurrent: int = 50
    gas_limit: int = 21000


@dataclass
class WalletRecoveryInfo:
    """Information about wallet recovery."""
    wallet_number: int
    private_key: str
    address: str
    initial_balance: int
    recoverable_amount: int
    gas_cost: int
    recovery_tx_hash: str = ""
    recovery_success: bool = False
    recovery_error: str = ""
    final_balance: int = 0
    recovery_block: int = 0


class WalletRecoveryTool:
    """Tool for recovering funds from fan-out wallets."""

    def __init__(self, config: Config):
        self.config = config
        self.console = Console()
        self.w3: Optional[AsyncWeb3] = None
        self.master_account = None
        self.recovery_results: List[WalletRecoveryInfo] = []
        self.artifacts_dir = Path("recovery_artifacts")
        self.artifacts_dir.mkdir(exist_ok=True)

    async def initialize(self) -> None:
        """Initialize the recovery tool."""
        self.console.print(Panel.fit("💰 Funds Recovery Tool", style="bold blue"))

        # Setup Web3 connection
        self.console.print("🔌 Connecting to blockchain...", style="yellow")
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.config.rpc_url))

        # Verify connection
        is_connected = await self.w3.is_connected()
        if not is_connected:
            raise ConnectionError("Failed to connect to blockchain")

        # Get chain ID and block number
        chain_id = await self.w3.eth.chain_id
        block_number = await self.w3.eth.block_number

        self.console.print(f"✅ Connected to blockchain", style="green")
        self.console.print(f"   Chain ID: {chain_id}", style="dim")
        self.console.print(f"   Current block: {block_number:,}", style="dim")

        # Setup master account
        self.master_account = Account.from_key(self.config.private_key)
        self.console.print(f"👛 Master wallet: {self.master_account.address}", style="cyan")

        # Check master balance
        balance = await self.w3.eth.get_balance(self.master_account.address)
        balance_eth = self.w3.from_wei(balance, 'ether')
        self.console.print(f"💰 Master balance: {balance_eth:.6f} ETH", style="green")

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.w3:
            try:
                if hasattr(self.w3.provider, '_session'):
                    if self.w3.provider._session and not self.w3.provider._session.closed:
                        await self.w3.provider._session.close()
                elif hasattr(self.w3.provider, 'session'):
                    if self.w3.provider.session and not self.w3.provider.session.closed:
                        await self.w3.provider.session.close()
            except Exception as e:
                self.console.print(f"⚠️  Error during cleanup: {e}", style="yellow")

    def generate_wallets(self, num_wallets: int) -> List[WalletRecoveryInfo]:
        """Generate wallet addresses by incrementing the master private key."""
        self.console.print(f"\n🔧 Generating {num_wallets} wallet addresses...", style="yellow")

        wallets = []
        master_key_int = int(self.config.private_key, 16)

        for i in range(1, num_wallets + 1):
            # Increment private key
            new_key_int = master_key_int + i
            new_private_key = f"{new_key_int:064x}"

            # Create account
            account = Account.from_key(new_private_key)

            wallet_info = WalletRecoveryInfo(
                wallet_number=i,
                private_key=new_private_key,
                address=account.address,
                initial_balance=0,
                recoverable_amount=0,
                gas_cost=0
            )
            wallets.append(wallet_info)

        return wallets

    async def check_wallet_balance(self, wallet: WalletRecoveryInfo) -> None:
        """Check the balance of a wallet and calculate recoverable amount."""
        balance = await self.w3.eth.get_balance(wallet.address)
        wallet.initial_balance = balance

        # Calculate gas cost for recovery transaction
        wallet.gas_cost = self.config.gas_limit * self.config.gas_price

        # Calculate recoverable amount (balance minus gas cost)
        if balance > wallet.gas_cost:
            wallet.recoverable_amount = balance - wallet.gas_cost
        else:
            wallet.recoverable_amount = 0

    async def wait_for_transaction_receipt(self, tx_hash: str, timeout: int = 60) -> Dict:
        """Wait for transaction receipt with detailed logging."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                receipt = await self.w3.eth.get_transaction_receipt(tx_hash)

                if receipt:
                    self.console.print(f"   📤 Transaction {tx_hash} confirmed", style="green")
                    return receipt
                else:
                    await asyncio.sleep(1)

            except Exception as e:
                self.console.print(
                    f"   📤 Transaction {tx_hash} not yet confirmed ({time.time() - start_time:.2f}s elapsed)", style="dim")
                self.console.print(f"      (Error: {e})", style="dim")
                await asyncio.sleep(1)

        raise TimeoutError(f"Transaction {tx_hash} not confirmed after {timeout} seconds")

    async def recover_funds_from_wallet(self, wallet: WalletRecoveryInfo) -> bool:
        """Recover funds from a single wallet with detailed logging."""
        try:
            # Create account from private key
            account = Account.from_key(wallet.private_key)

            # Get current nonce
            nonce = await self.w3.eth.get_transaction_count(account.address)

            self.console.print(f"   📤 Sending recovery transaction...", style="dim")
            self.console.print(
                f"      Amount: {self.w3.from_wei(wallet.recoverable_amount, 'ether'):.6f} ETH", style="dim")
            self.console.print(
                f"      Gas: {self.config.gas_limit} @ {self.w3.from_wei(self.config.gas_price, 'gwei'):.1f} gwei", style="dim")
            self.console.print(f"      Nonce: {nonce}", style="dim")

            # Build recovery transaction
            transaction = {
                'to': self.master_account.address,
                'value': wallet.recoverable_amount,
                'gas': self.config.gas_limit,
                'gasPrice': self.config.gas_price,
                'nonce': nonce,
                'chainId': self.config.chain_id
            }

            # Sign transaction
            signed_txn = account.sign_transaction(transaction)
            raw_transaction = getattr(signed_txn, 'raw_transaction',
                                      getattr(signed_txn, 'rawTransaction', None))

            # Send transaction
            tx_hash = await self.w3.eth.send_raw_transaction(raw_transaction)
            wallet.recovery_tx_hash = tx_hash.hex()

            self.console.print(f"   📨 Transaction sent: {wallet.recovery_tx_hash[:16]}...", style="dim cyan")

            # Wait for receipt
            self.console.print(f"   ⏳ Waiting for confirmation...", style="dim")
            receipt = await self.wait_for_transaction_receipt(wallet.recovery_tx_hash)

            # Check if transaction was successful
            if receipt['status'] == 1:
                wallet.recovery_success = True
                wallet.recovery_block = receipt['blockNumber']

                # Get final balance
                wallet.final_balance = await self.w3.eth.get_balance(wallet.address)

                self.console.print(f"   ✅ Recovery successful in block {receipt['blockNumber']:,}", style="green")
                self.console.print(f"      Gas used: {receipt['gasUsed']:,}", style="dim")
                self.console.print(
                    f"      Final balance: {self.w3.from_wei(wallet.final_balance, 'ether'):.9f} ETH", style="dim")

                return True
            else:
                wallet.recovery_error = "Transaction reverted"
                wallet.recovery_success = False
                self.console.print(f"   ❌ Transaction reverted", style="red")
                return False

        except Exception as e:
            wallet.recovery_error = str(e)
            wallet.recovery_success = False
            self.console.print(f"   ❌ Recovery failed: {e}", style="red")
            return False

    async def scan_and_recover_wallets(self, num_wallets: int) -> None:
        """Scan wallets and recover funds."""
        # Generate wallets
        wallets = self.generate_wallets(num_wallets)

        # Phase 1: Check all wallet balances
        self.console.print(f"\n📊 Phase 1: Checking wallet balances...", style="bold yellow")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Checking balances...", total=len(wallets))

            for wallet in wallets:
                await self.check_wallet_balance(wallet)
                progress.update(task, advance=1)

        # Display balance summary
        wallets_with_funds = [w for w in wallets if w.initial_balance > 0]
        recoverable_wallets = [w for w in wallets if w.recoverable_amount > 0]

        self.console.print(f"\n📈 Balance Summary:", style="bold cyan")
        self.console.print(f"   Total wallets scanned: {len(wallets)}")
        self.console.print(f"   Wallets with balance: {len(wallets_with_funds)}")
        self.console.print(f"   Wallets recoverable: {len(recoverable_wallets)}")

        if not wallets_with_funds:
            self.console.print("\n✨ No wallets have any balance. Nothing to recover.", style="green")
            self.recovery_results = wallets
            return

        # Display detailed wallet status
        wallet_table = Table(show_header=True, header_style="bold magenta", title="💼 Wallet Status")
        wallet_table.add_column("#", style="cyan", no_wrap=True)
        wallet_table.add_column("Address", style="dim")
        wallet_table.add_column("Balance (ETH)", style="yellow")
        wallet_table.add_column("Recoverable (ETH)", style="green")
        wallet_table.add_column("Status", style="white")

        total_balance = 0
        total_recoverable = 0

        for wallet in wallets_with_funds:
            balance_eth = self.w3.from_wei(wallet.initial_balance, 'ether')
            recoverable_eth = self.w3.from_wei(wallet.recoverable_amount, 'ether')

            total_balance += wallet.initial_balance
            total_recoverable += wallet.recoverable_amount

            status = "✅ Can recover" if wallet.recoverable_amount > 0 else "⚠️  Insufficient for gas"

            wallet_table.add_row(
                str(wallet.wallet_number),
                wallet.address[:10] + "..." + wallet.address[-8:],
                f"{balance_eth:.9f}",
                f"{recoverable_eth:.9f}",
                status
            )

        # Add totals row
        wallet_table.add_row(
            "TOTAL",
            "─" * 20,
            f"{self.w3.from_wei(total_balance, 'ether'):.9f}",
            f"{self.w3.from_wei(total_recoverable, 'ether'):.9f}",
            "─" * 20,
            style="bold"
        )

        self.console.print(wallet_table)

        if not recoverable_wallets:
            self.console.print("\n⚠️  No wallets have enough balance to cover gas costs.", style="yellow")
            self.recovery_results = wallets
            return

        # Phase 2: Recover funds
        self.console.print(
            f"\n💸 Phase 2: Recovering funds from {len(recoverable_wallets)} wallets...", style="bold yellow")

        successful_recoveries = 0
        failed_recoveries = 0
        total_recovered = 0

        for i, wallet in enumerate(recoverable_wallets):
            self.console.print(f"\n🔄 Wallet {wallet.wallet_number}/{num_wallets} ({wallet.address}):", style="cyan")
            self.console.print(f"   Balance: {self.w3.from_wei(wallet.initial_balance, 'ether'):.9f} ETH", style="dim")
            self.console.print(
                f"   Recoverable: {self.w3.from_wei(wallet.recoverable_amount, 'ether'):.9f} ETH", style="dim")

            success = await self.recover_funds_from_wallet(wallet)

            if success:
                successful_recoveries += 1
                total_recovered += wallet.recoverable_amount
            else:
                failed_recoveries += 1

            # Small delay between transactions
            if i < len(recoverable_wallets) - 1:
                await asyncio.sleep(0.5)

        # Store results
        self.recovery_results = wallets

        # Display final summary
        self.console.print(f"\n" + "="*60)
        self.console.print(Panel.fit("📊 Recovery Complete", style="bold green"))

        summary_table = Table(show_header=True, header_style="bold magenta")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")

        summary_table.add_row("Total wallets scanned", f"{len(wallets)}")
        summary_table.add_row("Wallets with funds", f"{len(wallets_with_funds)}")
        summary_table.add_row("Recovery attempts", f"{len(recoverable_wallets)}")
        summary_table.add_row("Successful recoveries", f"{successful_recoveries}")
        summary_table.add_row("Failed recoveries", f"{failed_recoveries}")
        summary_table.add_row("Total recovered", f"{self.w3.from_wei(total_recovered, 'ether'):.9f} ETH")

        self.console.print(summary_table)

        # Check for any remaining balances
        self.console.print(f"\n🔍 Final verification...", style="yellow")

        remaining_balance = 0
        wallets_with_remaining = []

        for wallet in wallets:
            current_balance = await self.w3.eth.get_balance(wallet.address)
            if current_balance > 0:
                remaining_balance += current_balance
                wallets_with_remaining.append((wallet.wallet_number, wallet.address, current_balance))

        if wallets_with_remaining:
            self.console.print(f"\n⚠️  Some wallets still have remaining balance:", style="yellow")
            for num, addr, balance in wallets_with_remaining:
                self.console.print(
                    f"   Wallet {num}: {self.w3.from_wei(balance, 'ether'):.9f} ETH ({addr[:10]}...)", style="dim")
            self.console.print(
                f"   Total remaining: {self.w3.from_wei(remaining_balance, 'ether'):.9f} ETH", style="yellow")
        else:
            self.console.print(f"✅ All recoverable funds have been successfully recovered!", style="green")

    def save_recovery_report(self) -> None:
        """Save detailed recovery report."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.artifacts_dir / f"recovery_report_{timestamp}.txt"

        with open(report_file, 'w') as f:
            f.write(f"Wallet Recovery Report - {timestamp}\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Master Wallet: {self.master_account.address}\n")
            f.write(f"Total Wallets Scanned: {len(self.recovery_results)}\n\n")

            f.write("Detailed Recovery Results:\n")
            f.write("-" * 60 + "\n")

            for wallet in self.recovery_results:
                f.write(f"\nWallet {wallet.wallet_number}:\n")
                f.write(f"  Address: {wallet.address}\n")
                f.write(f"  Initial Balance: {self.w3.from_wei(wallet.initial_balance, 'ether'):.9f} ETH\n")
                f.write(f"  Recoverable: {self.w3.from_wei(wallet.recoverable_amount, 'ether'):.9f} ETH\n")

                if wallet.recovery_tx_hash:
                    f.write(f"  Recovery TX: {wallet.recovery_tx_hash}\n")
                    f.write(f"  Recovery Success: {wallet.recovery_success}\n")
                    if wallet.recovery_success:
                        f.write(f"  Recovery Block: {wallet.recovery_block}\n")
                    if wallet.recovery_error:
                        f.write(f"  Error: {wallet.recovery_error}\n")
                    f.write(f"  Final Balance: {self.w3.from_wei(wallet.final_balance, 'ether'):.9f} ETH\n")
                else:
                    f.write(f"  No recovery attempted (insufficient funds)\n")

        self.console.print(f"\n📄 Recovery report saved to: {report_file}", style="green")


def load_configuration(config_path: str) -> Config:
    """Load configuration from environment file."""
    console = Console()

    if not os.path.exists(config_path):
        console.print(f"❌ Config file not found: {config_path}", style="red")
        console.print("Please create a config file with the following format:", style="yellow")
        console.print("""
RPC_URL=https://your-rpc-url.com
CHAIN_ID=1
PRIVATE_KEY=your_private_key_here
GAS_PRICE=200000000000
GAS_LIMIT=21000
        """)
        exit(1)

    # Load environment variables
    load_dotenv(config_path)

    config = Config(
        rpc_url=os.getenv('RPC_URL', Config.rpc_url),
        chain_id=int(os.getenv('CHAIN_ID', Config.chain_id)),
        private_key=os.getenv('PRIVATE_KEY', ''),
        to_address=os.getenv('TO_ADDRESS', ''),  # Not used but kept for compatibility
        gas_price=int(os.getenv('GAS_PRICE', Config.gas_price)),
        max_concurrent=int(os.getenv('MAX_CONCURRENT', Config.max_concurrent)),
        gas_limit=int(os.getenv('GAS_LIMIT', Config.gas_limit))
    )

    # Validate required fields
    if not config.private_key or config.private_key == 'your_private_key_here':
        console.print("❌ PRIVATE_KEY not set in config file", style="red")
        exit(1)

    return config


async def main():
    """Main function for wallet recovery."""
    parser = argparse.ArgumentParser(
        description="💰 Funds Recovery Tool - Recover funds from fan-out wallets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python recovery.py --wallets 5 --config .config
  python recovery.py --wallets 10 --config mainnet.config
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
        help='Number of wallets to check and recover'
    )

    args = parser.parse_args()

    console = Console()

    try:
        # Load configuration
        config = load_configuration(args.config)

        # Initialize recovery tool
        recovery_tool = WalletRecoveryTool(config)
        await recovery_tool.initialize()

        # Scan and recover wallets
        await recovery_tool.scan_and_recover_wallets(args.wallets)

        # Save report
        recovery_tool.save_recovery_report()

    except KeyboardInterrupt:
        console.print("\n🛑 Recovery interrupted by user", style="yellow")
    except Exception as e:
        console.print(f"\n❌ Recovery failed: {e}", style="red")
        raise
    finally:
        if 'recovery_tool' in locals():
            await recovery_tool.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
