# Quick Start

Follow these steps to set up, build, and run the Valkey server with the valkey-bloom module. This guide will walk you through creating a bloom filter, inserting items, and checking for items in the filters.

## Step 1: Install Valkey and valkey-bloom

1. Build Valkey from source by following the instructions [here](https://github.com/valkey-io/valkey?tab=readme-ov-file#building-valkey-using-makefile). Make sure to use Valkey version 8.0 or above.

2. Build the valkey-bloom module from source by following the instructions [here](https://github.com/valkey-io/valkey-bloom/blob/unstable/README.md#build-instructions).

## Step 2: Run the Valkey Server with valkey-bloom

Once valkey-bloom is built, run the Valkey server with the module loaded:

In case of Linux:
```bash
./valkey-server --loadmodule ./target/release/libvalkey_bloom.so
```

You should see the Valkey server start, and it will be ready to accept commands.

## Step 3: Create a Bloom Filter

Start a Valkey CLI session:

```bash
valkey-cli
```

Create a bloom filter using the BF.ADD, BF.INSERT, BF.RESERVE or BF.MADD commands. For example:

```bash
BF.ADD filter-key item-val
```

- `filter-key` is the name of the bloom filter we will be operating on
- `item-val` is the item we are inserting into the bloom filter

## Step 4: Insert some more items

To insert items on an already created filter, use the `BF.ADD`, `BF.MADD` or `BF.INSERT` commands:

```bash
BF.ADD filter-key example 
BF.MADD filter-key example1 example2 
```

Replace the example with the actual items you want to add.

## Step 5: Check if items are present

Now that you've created a bloom filter and inserted items, you can check what items have been added. Use the `BF.EXISTS` or `BF.MEXISTS` commands to check for items:

```bash
BF.EXISTS filter-key example
```

This command checks if an item is present in a bloom filter. Bloom filters can have false positives, but no false negatives. This means that if the BF.EXISTS command returns 0, then the item is not present. But if the BF.EXISTS command returns 1, there is a possibility (determined by false positive rate) that the item is not actually present.
