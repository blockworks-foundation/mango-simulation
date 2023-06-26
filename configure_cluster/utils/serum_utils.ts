import { Market, DexInstructions, OpenOrders,  } from "@project-serum/serum";
import {
    Connection,
    Keypair,
    LAMPORTS_PER_SOL,
    PublicKey,
    SystemProgram,
    TransactionInstruction,
    Transaction,
    sendAndConfirmTransaction,
    Signer,
} from "@solana/web3.js";

import { BN } from "@project-serum/anchor";
import * as splToken from "@solana/spl-token";

export class SerumUtils {
    private authority: Keypair;
    private payer : PublicKey;
    private conn: Connection;
    private dexProgramId : PublicKey;
    constructor(
        conn: Connection,
        authority: Keypair,
        dexProgramId : PublicKey) {
        this.conn = conn;
        this.authority = authority;
        this.payer = authority.publicKey;
        this.dexProgramId = dexProgramId;
    }

    private async createAccountIx(
        account: PublicKey,
        space: number,
        programId: PublicKey
    ): Promise<TransactionInstruction> {
        return SystemProgram.createAccount({
            newAccountPubkey: account,
            fromPubkey: this.payer,
            lamports: await this.conn.getMinimumBalanceForRentExemption(space),
            space,
            programId,
        });
    }

    async createAccount( owner : Keypair, pid : PublicKey, space: number): Promise<Keypair> {
        const newAccount = new Keypair();
        const createTx = new Transaction().add(
            SystemProgram.createAccount({
                fromPubkey: owner.publicKey,
                newAccountPubkey: newAccount.publicKey,
                programId: pid,
                lamports: await this.conn.getMinimumBalanceForRentExemption(
                    space
                ),
                space,
            })
        );

        await sendAndConfirmTransaction(this.conn, createTx, [
            owner,
            newAccount,
        ]);
        return newAccount;
    }
    
    async createTokenAccount(mint: PublicKey, payer: Keypair, owner: PublicKey) : Promise<PublicKey> {
        return splToken.createAccount(this.conn,
            payer,
            mint,
            owner,)
    }

    async transaction(): Promise<Transaction> {
        return new Transaction({
            feePayer: this.authority.publicKey,
            recentBlockhash: (await this.conn.getRecentBlockhash()).blockhash,
        });
    }

    /**
     * Create a new Serum market
     */
    public async createMarket(info: CreateMarketInfo): Promise<Market> {
        const owner = this.authority;
        const market = await this.createAccount( owner, this.dexProgramId, Market.getLayout(this.dexProgramId).span,);
        const requestQueue = await this.createAccount( owner, this.dexProgramId, 5132);
        const eventQueue = await this.createAccount( owner, this.dexProgramId, 262156);
        const bids = await this.createAccount( owner, this.dexProgramId, 65548);
        const asks = await this.createAccount( owner, this.dexProgramId, 65548);
        const quoteDustThreshold = new BN(100);

        const [vaultOwner, vaultOwnerBump] = await this.findVaultOwner(
            market.publicKey
        );

        const [baseVault, quoteVault] = await Promise.all([
            splToken.createAccount(this.conn, this.authority, info.baseToken, vaultOwner, Keypair.generate()),
            splToken.createAccount(this.conn, this.authority, info.quoteToken, vaultOwner, Keypair.generate()),
            ]);
        
            
        const initMarketTx = (await this.transaction()).add(
            DexInstructions.initializeMarket(
                toPublicKeys({
                    market,
                    requestQueue,
                    eventQueue,
                    bids,
                    asks,
                    baseVault,
                    quoteVault,
                    baseMint: info.baseToken,
                    quoteMint: info.quoteToken,
                    baseLotSize: new BN(info.baseLotSize),
                    quoteLotSize: new BN(info.quoteLotSize),
                    feeRateBps: info.feeRateBps,
                    vaultSignerNonce: vaultOwnerBump,
                    quoteDustThreshold,
                    programId: this.dexProgramId,
                })
            )
        );

        await sendAndConfirmTransaction(this.conn, initMarketTx, [this.authority]);

        let mkt = await Market.load(
            this.conn,
            market.publicKey,
            { commitment: "recent" },
            this.dexProgramId
        );
        return mkt;
    }

    async createWallet(lamports: number): Promise<Keypair> {
        const wallet = Keypair.generate();
        const fundTx = new Transaction().add(
            SystemProgram.transfer({
                fromPubkey: this.authority.publicKey,
                toPubkey: wallet.publicKey,
                lamports,
            })
        );

        await sendAndConfirmTransaction(this.conn, fundTx, [this.authority]);
        return wallet;
    }

    public async getMarket(market: PublicKey) {
        return Market.load(this.conn, market, {commitment: "confirmed"}, this.dexProgramId);
    }

    public async createMarketMaker(
        lamports: number,
        tokens: [PublicKey, BN][]
    ): Promise<MarketMaker> {
        const account = await this.createWallet(lamports);
        const tokenAccounts = {};
        const transactions = [];
        for (const [token, amount] of tokens) {
            const publicKey = await this.createTokenAccount(
                token,
                this.authority,
                account.publicKey,
            );
            splToken.mintTo( this.conn, this.authority, token, publicKey, this.authority, amount.toNumber())
            tokenAccounts[token.toBase58()] = publicKey;
        }

        return new MarketMaker(account, tokenAccounts);
    }

    public async createAndMakeMarket(baseToken: PublicKey, quoteToken: PublicKey, marketPrice: number, exp : number): Promise<Market> {
        const market = await this.createMarket({
            baseToken,
            quoteToken,
            baseLotSize: 1000,
            quoteLotSize: 100,
            feeRateBps: 0,
        });
        let nb = Math.floor(40000/marketPrice);
        {
            
            const marketMaker = await this.createMarketMaker(
                1 * LAMPORTS_PER_SOL,
                [
                    [baseToken, new BN(nb * 10)],
                    [quoteToken,  new BN(nb * 10)],
                ]
            );
            const bids = MarketMaker.makeOrders([[marketPrice * 0.995, nb]]);
            const asks = MarketMaker.makeOrders([[marketPrice * 1.005, nb]]);

            await marketMaker.placeOrders(this.conn, market, bids, asks);
        }
        return market;
    }

    public async findVaultOwner(market: PublicKey): Promise<[PublicKey, BN]> {
        const bump = new BN(0);
    
        while (bump.toNumber() < 255) {
            try {
                const vaultOwner = await PublicKey.createProgramAddress(
                    [market.toBuffer(), bump.toArrayLike(Buffer, "le", 8)],
                    this.dexProgramId
                );
    
                return [vaultOwner, bump];
            } catch (_e) {
                bump.iaddn(1);
            }
        }
    
        throw new Error("no seed found for vault owner");
    }
    
}

export interface CreateMarketInfo {
    baseToken: PublicKey;
    quoteToken: PublicKey;
    baseLotSize: number;
    quoteLotSize: number;
    feeRateBps: number;
}

export interface Order {
    price: number;
    size: number;
}

export class MarketMaker {
    public account: Keypair;
    public tokenAccounts: { [mint: string]: PublicKey };

    constructor(
        account: Keypair,
        tokenAccounts: { [mint: string]: PublicKey }
    ) {
        this.account = account;
        this.tokenAccounts = tokenAccounts;
    }

    static makeOrders(orders: [number, number][]): Order[] {
        return orders.map(([price, size]) => ({ price, size }));
    }

    async placeOrders(connection : Connection, market: Market, bids: Order[], asks: Order[]) {
        await connection.confirmTransaction(
            await connection.requestAirdrop(this.account.publicKey, 20 * LAMPORTS_PER_SOL),
            "confirmed"
          );

        const baseTokenAccount =
            this.tokenAccounts[market.baseMintAddress.toBase58()];

        const quoteTokenAccount =
            this.tokenAccounts[market.quoteMintAddress.toBase58()];

        const askOrderTxs = [];
        const bidOrderTxs = [];

        const placeOrderDefaultParams = {
            owner: this.account.publicKey,
            clientId: undefined,
            openOrdersAddressKey: undefined,
            openOrdersAccount: undefined,
            feeDiscountPubkey: null,
        };
        for (const entry of asks) {
            const { transaction, signers } =
                await market.makePlaceOrderTransaction(
                    connection,
                    {
                        payer: baseTokenAccount,
                        side: "sell",
                        price: entry.price,
                        size: entry.size,
                        orderType: "limit",
                        selfTradeBehavior: "decrementTake",
                        ...placeOrderDefaultParams,
                    }
                );

            askOrderTxs.push([transaction, [this.account, ...signers]]);
        }

        for (const entry of bids) {
            const { transaction, signers } =
                await market.makePlaceOrderTransaction(
                    connection,
                    {
                        payer: quoteTokenAccount,
                        side: "buy",
                        price: entry.price,
                        size: entry.size,
                        orderType: "limit",
                        selfTradeBehavior: "decrementTake",
                        ...placeOrderDefaultParams,
                    }
                );

            bidOrderTxs.push([transaction, [this.account, ...signers]]);
        }

        await this.sendAndConfirmTransactionSet(
            connection,
            ...askOrderTxs,
            ...bidOrderTxs
        );
    }

    async  sendAndConfirmTransactionSet(
        connection: Connection,
        ...transactions: [Transaction, Signer[]][]
    ): Promise<string[]> {
        const signatures = await Promise.all(
            transactions.map(([t, s]) =>
            connection.sendTransaction(t, s)
            )
        );
        const result = await Promise.all(
            signatures.map((s) => connection.confirmTransaction(s))
        );

        const failedTx = result.filter((r) => r.value.err != null);

        if (failedTx.length > 0) {
            throw new Error(`Transactions failed: ${failedTx}`);
        }

        return signatures;
    }
}

export function toPublicKeys(
    obj: Record<string, string | PublicKey | HasPublicKey | any>
): any {
    const newObj = {};

    for (const key in obj) {
        const value = obj[key];

        if (typeof value == "string") {
            newObj[key] = new PublicKey(value);
        } else if (typeof value == "object" && "publicKey" in value) {
            newObj[key] = value.publicKey;
        } else {
            newObj[key] = value;
        }
    }

    return newObj;
}

interface HasPublicKey {
    publicKey: PublicKey;
}