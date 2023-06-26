# Configure Mango - A faster and easier way to configure Mango Markets on a cluster

This code can be used to configure mango group, tokens, spot market, perp markets and oracles on local solana validator and create 50 mango user accounts.
It will create authority.json which is keypair of authority file, accounts.json which contains all user data and ids.json which contains info about mango group.

The project also contains necessary binary files. You can always update the binary files by compiling from source and replacing the binaries locally. You have to apply bin/mango.patch to your mango repository to use it to create a local cluster.
From the root directory of this project do:

## How to use

To install all the dependencies :
```sh
yarn install
```

To start a solana test validator
```sh
sh scripts/start_test_validator.sh
```
Or
To start a local solana validator
```sh
sh scripts/configure_local.sh
```

To configure mango
```sh
yarn ts-node index.ts
```

To run mango keeper (deprecated)
```sh
yarn ts-node keeper.ts
```

To create 50 users and store in the file accounts.json
```sh
ts-node create-users.ts 50 accounts.json
```

To refund users in account file with some sols
```sh
ts-node refund_users.ts accounts.json 
```

Pyth oracle is a mock it is a program which will just rewrite an account with a binary data.