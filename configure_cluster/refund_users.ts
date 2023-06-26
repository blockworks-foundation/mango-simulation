import { web3 } from "@project-serum/anchor";
import { Cluster, Connection, Keypair, LAMPORTS_PER_SOL, SystemProgram } from "@solana/web3.js";
import { sleep } from "@blockworks-foundation/mango-client";

if (process.argv.length < 3) {
    console.log("please enter user file name as argument");
}
import { readFileSync }  from 'fs';
let fileName = process.argv[2];

interface Users {
    publicKey: string;
    secretKey: any;
    mangoAccountPks: string,
}

enum Result {
    SUCCESS = 0,
    FAILURE = 1
};


export async function main(users: Users[],
                           authority: Keypair,
                           targetBalance: number,
                           n_try: number) {
    // cluster should be in 'devnet' | 'mainnet' | 'localnet' | 'testnet'  
    const endpoint = process.env.ENDPOINT_URL || 'http://localhost:8899';
    const connection = new Connection(endpoint, 'confirmed');

    let accounts_to_fund = new Array<[Users, number]>();
    try {
        for (let i = 0; i <= n_try; i++) {
            if (i > 0) {
                await sleep(2000);
            }
            // add all accounts which have balance less than threshold to set
            {
                let promises : Promise<number>[]= []
                for (let curAccount of users) {
                    let curPubkey = new web3.PublicKey(curAccount.publicKey)
                    promises.push(connection.getBalance(curPubkey));
                }
                const balance = await Promise.all(promises);
                users.forEach( (cur_account, index) =>  {
                    if (balance[index] < LAMPORTS_PER_SOL * targetBalance) {
                        accounts_to_fund.push([cur_account, balance[index]]);
                    }
                });
                if (accounts_to_fund.length == 0) {
                    return Result.SUCCESS; 
                }
            }
            // fund all these accounts
            {
                let promises : Promise<String>[]= []
                let blockHash = await connection.getLatestBlockhash();
                for (const [user, balance] of accounts_to_fund) {
                    let userPubkey = new web3.PublicKey(user.publicKey)
                    const ix = SystemProgram.transfer({
                        fromPubkey: authority.publicKey,
                        lamports: LAMPORTS_PER_SOL * targetBalance - balance,
                        toPubkey: userPubkey,
                    })
                    let tx = new web3.Transaction().add(ix);
                    tx.recentBlockhash = blockHash.blockhash;
                    console.log("Fund for " + (LAMPORTS_PER_SOL * targetBalance - balance)/LAMPORTS_PER_SOL + "");
                    promises.push(connection.sendTransaction(tx, [authority]))
                }

                try {
                    const result = await Promise.all(promises);
                } catch(e) {
                    console.log('While sending transactions caught an error : ' + e + ". Will try again.")
                }
            }
            accounts_to_fund.length = 0;
        }
    } catch(e) {
        console.log('caught an error : ' + e)
    }
    return Result.FAILURE;
}


const targetBalance = parseFloat(process.env.REFUND_TARGET_SOL || '1.0');
const nTry = parseFloat(process.env.NUMBER_TRY_REFUNG || '3');
const file = readFileSync(fileName, 'utf-8');
const users : Users[] = JSON.parse(file);
if (users === undefined) {
    console.log("cannot read users list")
}
const authority = Keypair.fromSecretKey(
    Uint8Array.from(
        JSON.parse(
            process.env.KEYPAIR ||
                readFileSync('authority.json', 'utf-8'),
        ),
    ),
);
console.log('refunding to have up to ' + targetBalance + ' sol for ' + users.length + ' users')
main(users, authority, targetBalance, nTry).then(x => {
    if (x == Result.SUCCESS) {
        console.log('finished sucessfully')
        process.exit(0);
    } else {
        console.log('failed')
        process.exit(1);
    }
});
