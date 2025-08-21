# Camino Stress Testing Tool

## Install Dependencies

```
pip install -r requirements.txt
```

## Configuration

There is a `.config.example` file you should copy as `.config` and edit to set your
values for wallet private key, RPC URL, and `to` address.

```
cp .config.example .config
```

You can also use the `--config <filepath>` option with the scripts to use different
configurations for different runs.

## Single Wallet Test

The `camino_stress.py` script is for a single wallet tests.

### Run with default values

This will run with default values of 1000 transactions and using `.config` file.

```
python3 camino_stress.py
```

### Run with custom values

This will run for 5000 transactions and using `.config.local` file.

```
python3 camino_stress.py --count 5000 --config .config.local
```

## Multi-Wallet Test

The `fanout_stress.py` script is for a multi wallet tests. This uses the wallet from
the config and generates multiple wallets from that by incrementing the last digit.

Then distributes funds to those newly generated wallets and uses these wallets for
running the stress test in parallel.

> [!NOTE]  
> Please note that public API nodes are generally rate-limited. If you want to get
> the full speed use your own node.

### Example run

This will run 4 wallets and 500 transactions each.

```
python3 fanout_stress.py --wallets 4 --count 500
```

## Funds Recovery

The fanout script does a funds recovery from the generated wallets back to the
master wallet. But sometimes (especially when you run for too many wallets in
paralle) some funds are left in the generated wallets.

The `recover_funds.py` script is used to recover those back into the master wallet.
Run this script with same wallet number (or higher) to check for left over funds and
recover them.

### Example run

```
python3 recover_funds.py --wallets 4
```
